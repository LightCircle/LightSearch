/**
 * @file 検索 (fulltext/similar search)
 *
 * exports function
 *     batch     : インデックス一括作成
 *     patch     : インデックス個別作成
 *     search    : 類似検索
 *     condition : 一致検索条件作成
 *
 * @author exabugs@gmail.com
 * @copyright Dreamarts Corporation. All Rights Reserved.
 */

"use strict";

var async = require("async")
  , errors = require("../error").http
  , log = require("../log")
  , path = require("path")
  , fs = require("fs")
  , MeCab = new require("./mecab")
  , connection = require("../connection")
  , constant = require("../constant")
  , conf = require("config").app
  ;

// 類似検索のインデックスとしてはヒットの可能性を広げるためにnbest=2とする
var Nbest = 1;

// 辞書ディレクトリ
var dicdir = conf.mecabDictionary || "dic";

// コレクション拡張子
var _idf = "_idf"; // 単語辞書テーブル拡張子
var _similarity = "_similarity"; // 類似度テーブル拡張子

// コレクション(mongooseの複数系対応)
var COLLECTION_SUFFIX = "s";

/**
 * ユーザ辞書作成
 * @param code
 * @param lang
 * @param vs
 * @param callback
 */
exports.makeDictionary = function (code, lang, words, callback) {
  var tasks = [];
  tasks.push(function (done) {
    var file = path.resolve(path.join(dicdir, lang, 'user.csv'));
    var write_stream = fs.createWriteStream(file);
    write_stream
      .on('error', function (err) {
        return callback(new errors.InternalServer(err));
      })
      .on('close', function () {
        done(null, file);
      });
    words.forEach(function (w) {
      write_stream.write(w.title + ",");
      write_stream.write(","); // 左文脈ID
      write_stream.write(","); // 右文脈ID
      write_stream.write((w.cost || 0) + ",");
      write_stream.write((w.lexical || "*") + ",");
      write_stream.write((w.compound1 || "*") + ",");
      write_stream.write((w.compound2 || "*") + ",");
      write_stream.write((w.compound3 || "*") + ",");
      write_stream.write((w.conjugation || "*") + ",");
      write_stream.write((w.inflection || "*") + ",");
      write_stream.write((w.original || "*") + ",");
      write_stream.write((w.reading || "*") + ",");
      write_stream.write((w.pronunciation || "*") + "\n");
      // hint:バックスラッシュは「option」+「¥」
    });
    write_stream.end();
  });
  tasks.push(function (file, done) {
    var option = {
      "dicdir": path.resolve(path.join(dicdir, lang)),
      "charset": "utf8",
      "dictionary-charset": "utf8",
      "userdic": path.resolve(path.join(dicdir, lang, "user.dic")) // build user dictionary
    };
    MeCab.makeDictionary(file, option, function (err) {
      done(err);
    });
  });
  async.waterfall(tasks, function (err) {
    log.debug("finished: makeDictionary.");
    callback(err);
  });
}

/**
 * 単語分割とTF計算
 * @param infos [ [ "hogehoge", 10], [ "....", 1 ] ]
 * @param callback
 */
function parse(lang, infos, nbest, callback) {

  // pos : parts of speech (品詞)
  // pm : punctuation marks (句読点) 句読点で区切って形態素解析する
  var filters_i18n =
  {
    "ja": {
      pos: {
        "名詞": {"一般": 1, "固有名詞": 1, "数": 1, "サ変接続": 1, "形容動詞語幹": 1, "副詞可能": 1}
      },
      pm: /[。、]/
    },
    "zh": {
      pos: {
        "名詞": {"一般": 1, "固有名詞": 1}
      },
      pm: /[。、]/
    }
  };

  var filters = filters_i18n[lang];

  var mecab = new MeCab();

  var option = {
    "dicdir": path.resolve(path.join(dicdir, lang)),
    "nbest": nbest
  };

  var userdic = path.resolve(path.join(dicdir, lang, "user.dic"));
  if (path.existsSync(userdic)) {
    option.userdic = userdic;
  }

  var N = 0;
  var terms = {}, count = 0;
  var poss = filters["pos"];
  var pm = filters["pm"];
  infos.forEach(function (info) {
    var string = info[0] || "";
    var weight = info[1];
    var texts = string.split(pm);
    N += texts.length;
    texts.forEach(function (text) {
      mecab.parse(text, option, function (err, result) {
        if (err) throw err;

        for (var i = 0, len = result.length; i < len; i++) {
          var term = result[i];
          var pos = poss[term[1]];
          if (pos && pos[term[2]]) {
            var word = (term[7] === "*") ? term[0] : term[7]; // オリジナル(もし動詞の場合は基本形)
            var read = (term[8] === "*") ? term[0] : term[8]; // 読み(カタカナ)
            if (!terms[word]) terms[word] = {w: 0, r: {}};
            var tgt = terms[word];
            tgt.w |= 0;
            tgt.w += weight;
            count += weight;
            if (word) { // wordも「読み」部分へ入れておく。検索の利便性のため。
              tgt.r[word] |= 0;
              tgt.r[word] += 1;
            }
            if (read) {
              tgt.r[read] |= 0;
              tgt.r[read] += 1;
            }
          }
        }

        if (--N == 0) {
          var ret1 = [];
          var ret2 = [];
          for (var key in terms) {
            ret1.push(key);
            ret2.push({k: key, v: terms[key].w / count, r: terms[key].r});
          }
          //console.log(ret1);
          //console.log(ret2);
          callback(err, ret1, ret2, count);
        }
      });
    });
  });
}


// Utility
var util = {
  // product
  product: function (a, v) {
    var b = [];
    for (var i = 0, len = a.length; i < len; i++) {
      var ai = a[i];
      if (ai) {
        var w = v[ai.k] ? v[ai.k] * ai.v : 0;
        //b.push({k: ai.k, w: w, r: Object.keys(ai.r)}); 特に読みは必要ない
        b.push({k: ai.k, w: w});
      }
    }
    return b;
  },
  // norm
  norm: function (a) {
    var sum = 0;
    for (var i = 0, len = a.length; i < len; i++) {
      var w = a[i].w;
      if (w) {
        sum += w * w;
      }
    }
    return Math.sqrt(sum);
  },
  // innerproduct
  innerproduct: function (a, v) {
    var sum = 0;
    for (var i = 0, len = a.length; i < len; i++) {
      var info = a[i];
      if (v[info.k]) {
        sum += info.w * v[info.k];
      }
    }
    return sum;
  },
  // to_hash
  to_hash: function (a) {
    var ret = {};
    for (var i = 0, len = a.length; i < len; i++) {
      if (a[i].w) {
        ret[a[i].k] = a[i].w;
      }
    }
    return ret;
  },
  // to_array
  to_array: function (a) {
    var ret = [a.length];
    for (var i = 0, len = a.length; i < len; i++) {
      ret[i] = a[i].k;
    }
    return ret;
  }
};

/**
 *
 * @param collection
 * @param infos {title: 10, createBy: 1}
 * @param callback
 */
exports.batch = function (lang, collection, index_collection, infos, condition, callback) {

  var tasks = [];
  // Prepare TF table.
  tasks.push(function (done) {
    tf(lang, collection, infos, condition, function (err, table_tfidf) {
      if (err) {
        log.error(err);
        return done(new errors.InternalServer(err));
      }
      return done(err, table_tfidf);
    });
  });
  // IDF Dictionary
  tasks.push(function (table_tfidf, done) {
    dic(collection, table_tfidf, function (err, table_dic) {
      if (err) {
        log.error(err);
        return done(new errors.InternalServer(err));
      }
      return done(err, table_tfidf, table_dic);
    });
  });
  // prepare Dic
  tasks.push(function (table_tfidf, table_dic, done) {
    var dic = {};
    table_dic.find().each(function (err, idf) {
      if (idf == null) {
        return done(err, table_tfidf, dic);
      }
      dic[idf._id] = idf.value.w;
    });
  });
  // Calc TF-IDF
  tasks.push(function (table_tfidf, dic, done) {
    tfidf(table_tfidf, dic, function (err, table_tfidf) {
      if (err) {
        log.error(err);
        return done(new errors.InternalServer(err));
      }
      return done(err, table_tfidf);
    });
  });
  // Copy to original collection.
  tasks.push(function (table_tfidf, done) {
    table_tfidf.find().each(function (err, tgt) {
      if (tgt == null) {
        return done(err, table_tfidf);
      }
      index_collection.update({_id: tgt._id}, {$set: {tf: tgt.value}}, {upsert: true}, function (err, result) {
      });
    });
  });
  // Calcurate Cosine similarity.
  tasks.push(function (table_tfidf, done) {
    cosine(collection, table_tfidf, function (err, result) {
      if (err) {
        log.error(err);
        return done(new errors.InternalServer(err));
      }
      return done(err, table_tfidf, result);
    });
  });
  async.waterfall(tasks, function (err, table_tfidf, table_similarity) {

    if (err) {
      log.error(err);
      callback(err, null);
      return;
    }

    log.debug("finished: add message.");
    callback(err);
  });
}

/**
 *
 * @param collection
 * @param infos {title: 10, createBy: 1}
 * @param callback
 */
function tf(lang, collection, infos, condition, callback) {

  var master_name = collection.collectionName;
  var db = collection.db;

  var target_table = db.collection(master_name + '_tfidf');

  var N = 0;
  target_table.drop(function (err, reply) {
    collection.find(condition).each(function (err, src) {
      if (src != null) {
        // テキスト解析
        parse(lang, to_string(src, infos), Nbest, function (err, words, v, n) {
          ++N;
          target_table.insert({_id: src._id, value: {v: v, n: n} }, function (err) {
            if (--N == 0)
              return callback(err, target_table);
          });

        });
      }
    });
  });
}

/**
 * IDF Dictionary
 * @param collection
 * @param table_tfidf
 * @param callback
 */
function dic(collection, table_tfidf, callback) {

  var master_name = collection.collectionName;
  var db = collection.db;

  var map = function () {
    var v = this.value.v;
    if (!v) return;
    for (var i = 0, len = v.length; i < len; i++) {
      if (v[i]) {
        emit(v[i].k, {w: 1, r: v[i].r});
      }
    }
  };

  var reduce = function (key, values) {
    var total = {w: 0, r: {}};
    values.forEach(function (value) {
      total.w += 1;
      for (var key in value.r) {
        total.r[key] |= 0;
        total.r[key] += value.r[key];
      }
    });
    return total;
  };

  table_tfidf.count(function (err, N) {
    if (err != null) callback(err);
    table_tfidf.mapReduce(
      map,
      reduce,
      {
        scope: {N: N},
        finalize: function (key, value) {
          value.w = Math.log(N / value.w);
          value.r = Object.keys(value.r);
          return value;
        },
        out: {replace: master_name + _idf}
      },
      function (err, results) {
        if (err) {
          console.log(err);
          callback(err);
        }
        callback(err, results);
      }
    );
  });
}

/**
 *
 * @param collection
 * @param table_tfidf
 * @param dic
 * @param callback
 */
function tfidf(table_tfidf, dic, callback) {

  var map = function () {

    util.product = function (a, v) {
      var b = [];
      for (var i = 0, len = a.length; i < len; i++) {
        var ai = a[i];
        if (ai) {
          var w = v[ai.k] ? v[ai.k] * ai.v : 0;
          //b.push({k: ai.k, w: w, r: Object.keys(ai.r)}); 特に読みは必要ない
          b.push({k: ai.k, w: w});
        }
      }
      return b;
    };

    util.norm = function (a) {
      var sum = 0;
      for (var i = 0, len = a.length; i < len; i++) {
        if (a[i].w) {
          sum += a[i].w * a[i].w;
        }
      }
      return Math.sqrt(sum);
    };

    var v = util.product(this.value.v, dic);
    //var n = this.value.n; // 単語数
    //emit(this._id, {v: v, n: n, l: util.norm(v)});
    emit(this._id, {v: v, l: util.norm(v)});
  };

  var reduce = function (key, values) {
    return values[0];
  };

  table_tfidf.mapReduce(
    map,
    reduce,
    {
      scope: {util: util, dic: dic},
      out: {replace: table_tfidf.collectionName}
    },
    function (err, results) {
      if (err) {
        console.log(err);
        callback(err);
      }
      callback(err, results);
    }
  );

}

/**
 *
 * @param collection
 * @param table_tfidf
 * @param callback
 */
function cosine(collection, table_tfidf, callback) {

  var master_name = collection.collectionName;
  var db = collection.db;

  var target_table = db.collection(master_name + _similarity);

  target_table.drop(function (err, reply) {

    table_tfidf.find().each(function (err, src) {

      if (src == null) {
        return callback(err, collection);
      }
      src.value.condition = util.to_hash(src.value.v);
      var keys = util.to_array(src.value.v);

      table_tfidf.mapReduce(
        function () {

          util.innerproduct = function (a, v) {
            var sum = 0;
            for (var i = 0, len = a.length; i < len; i++) {
              var info = a[i];
              if (v[info.k]) {
                sum += info.w * v[info.k];
              }
            }
            return sum;
          };

          var s = util.innerproduct(this.value.v, src.value.condition);
          if (0 < s) {
            emit(this._id, s / this.value.l / src.value.l);
          }
        },
        function (key, values) {
          return values[0];
        },
        {
          scope: {util: util, src: src},
          query: {_id: {$gt: src._id}, "value.v.k": {$in: keys}},
          out: {inline: 1}
        },
        function (err, results) {
          if (err) {
            return callback(err, results);
          }
          results.forEach(function (dst) {
            target_table.insert({a: dst._id, b: src._id, score: dst.value}, function (err, a) {
            });
            target_table.insert({a: src._id, b: dst._id, score: dst.value}, function (err, a) {
            });
          });
        }
      );
    });
  });
}

/**
 * tfベクトルvを_idfテーブルを参照してtf-idfベクトルに変換する
 * @param collection
 * @param tf
 * @param callback
 */
function to_tfidf(code, collection, tf, callback) {

  var words = util.to_array(tf);

  var db = connection.db(code).collection(collection + _idf);
  db.find({_id: {$in: words}}).toArray(function (err, results) {

    var dic = {};
    results.forEach(function (tgt) {
      dic[tgt._id] = tgt.value.w;
    });

    tf = util.product(tf, dic)
    callback(err, tf, util.to_hash(tf), util.norm(tf));
  })
}

/**
 *
 * @param lang 言語
 * @param code
 * @param collection 元データを保存するコレクション
 * @param infos index対象フィールド名{title: 10, createBy: 1}
 * @param object 更新オブジェクト
 * @param callback
 */
exports.patch = function (lang, code, collection, infos, object, callback) {

  var collectionName = constant.MODULES_NAME_DATASTORE_PREFIX + collection;
  var indexCollectionName = collectionName + COLLECTION_SUFFIX;

  // テキスト解析
  parse(lang, to_string(object, infos), Nbest, function (err, words, v, n) {

    to_tfidf(code, collectionName.toLowerCase(), v, function (err, v, w, l) {

      var tf = {v: v, l: l};

      var db = connection.db(code).collection(indexCollectionName.toLowerCase());
      db.update({_id: object._id}, {$set: {tf: tf}}, {upsert: true}, function (err, result) {

        // TODO: エラーログ追加
        callback(err, result);
      });
    });
  });
}

/**
 * TFと単語だけを、全文検索用データとして入れる
 * @param lang
 * @param code
 * @param collection
 * @param infos
 * @param object
 * @param callback
 */
exports.patch1 = function (lang, code, collection, infos, object, callback) {

  var collectionName = constant.MODULES_NAME_DATASTORE_PREFIX + collection;
  var indexCollectionName = collectionName + COLLECTION_SUFFIX;

  // テキスト解析
  parse(lang, to_string(object, infos), Nbest, function (err, words, v, n) {

    // TODO: rの値の処理
    var db = connection.db(code).collection(indexCollectionName.toLowerCase());
    db.update({_id: object._id}, {$set: {tf: {v: v}}}, {upsert: true}, function (err, result) {

      // TODO: エラーログ追加
      callback(err, result);
    });
  });
};

/**
 * 類似検索の実行
 * @param lang
 * @param collection
 * @param out 出力コレクション名
 * @param condition
 * @param text 検索キーワード
 * @param callback
 */
exports.search1 = function (lang, collection, index_collection, out, condition, sort, text, callback) {

  if (!text) text = "";

  var tasks = [];
  tasks.push(function (done) {
    // テキスト解析
    var infos = [
      [text, 1]
    ];
    parse(lang, infos, Nbest, function (err, words, v, n) {
      if (err) {
        return  callback(err, collection.db.collection(out));
      } else {
        done(err, words, v);
      }
    });
  });
  tasks.push(function (words, v, done) {
    to_tfidf(collection, v, function (err, v, w, l) {
      if (err) {
        return  callback(err, collection.db.collection(out));
      } else {
        var tf = {w: w, l: l};
        done(err, words, tf);
      }
    });
  });
  tasks.push(function (words, tf, done) {

    var map = function () {
      util.innerproduct = function (a, v) {
        var sum = 0;
        for (var i = 0, len = a.length; i < len; i++) {
          var info = a[i];
          if (v[info.k]) {
            sum += info.w * v[info.k];
          }
        }
        return sum;
      };
      util.select = function (object, columns) {
        var result = {};
        columns.forEach(function (column) {
          result[column] = object[column];
        });
        return result;
      };
      var obj = util.select(this, sort);
      var sum = util.innerproduct(this.tf.v, tf.w);
      obj["_score"] = sum / this.tf.l / tf.l;
      emit(this._id, obj);
    };

    var reduce = function (key, values) {
      return values[0];
    };

    if (0 < words.length) {
      condition["tf.v.k"] = {$in: words};
    }

    index_collection.mapReduce(
      map,
      reduce,
      {
        scope: {util: util, sort: sort, tf: tf},
        query: condition,
        out: out
      },
      function (err, result) {
        if (err) {
          return done(new errors.InternalServer(err));
        } else {
          return done(err, result);
        }
      }
    );
  });
  async.waterfall(tasks, function (err, result) {
    if (err) {
      log.error(err);
      callback(err);
      return;
    }
    callback(err, result);
  });

}

/**
 * 完全一致検索用の条件生成
 * @param text
 * @param condition
 * @param callback
 */
exports.condition = function (lang, text, callback) {
  var nbest = 1; // 完全一致なので形態素解析の結果は最良を1つだけ使用する
  var infos = [
    [text, 1]
  ];
  parse(lang, infos, nbest, function (err, words, tf, i) {
    callback(null, words, i);
  });
}

/**
 * カラム名から実際の文字列に変換する
 * @param object
 * @param infos {title: 10, createBy: 1}
 * @returns {Array} [ [ "hogehoge", 10], [ "....", 1 ] ]
 */
function to_string(object, infos) {

  var results = [];
  for (var key in infos) {
    results.push([object._doc[key], infos[key]]);
  }
  return results;
}

/**
 * 通常検索
 * @param lang
 * @param collection
 * @param out 出力コレクション名
 * @param condition
 * @param sort ソートに使用するカラムの配列
 * @param callback
 */
exports.search0 = function (lang, collection, index_collection, out, condition, sort, text, callback) {

  if (!text) text = "";

  var tasks = [];
  tasks.push(function (done) {
    // テキスト解析
    var nbest = 1; // 完全一致なので形態素解析の結果は最良を1つだけ使用する
    var infos = [
      [text, 1]
    ];
    parse(lang, infos, nbest, function (err, words, v, n) {
      if (err) {

      } else {
        done(err, words);
      }
    });
  });
  tasks.push(function (words, done) {

    var map = function () {
      util.select = function (object, columns) {
        var result = {};
        columns.forEach(function (column) {
          result[column] = object[column];
        });
        return result;
      };
      var obj = util.select(this, sort);
      emit(this._id, obj);
    };

    var reduce = function (key, values) {
      return values[0];
    };

    if (0 < words.length) {
      condition["tf.v.k"] = {$all: words};
    }

    var db = connection.db(undefined).collection(index_collection);
    db.mapReduce(
      map,
      reduce,
      {
        scope: {util: util, sort: sort},
        query: condition,
        out: out
      },
      function (err, result) {
        if (err) {
          return done(new errors.InternalServer(err));
        } else {
          return done(err, result);
        }
      }
    );
  });
  async.waterfall(tasks, function (err, result) {
    if (err) {
      log.error(err);
      callback(err);
      return;
    }
    callback(err, result);
  });
};

/**
 * シンプルサーチ
 * @param lang
 * @param code
 * @param collection
 * @param text
 * @param callback
 */
exports.search3 = function (lang, code, collection, text, callback) {

  // テキスト解析
  var analyze = function (done) {
    parse(lang, [text || "", 1], 1, function (err, words) {
      done(err, words);
    });
  };

  // キーワードで検索、対象一覧のIDを返す
  var find = function (words, done) {

    var collectionName = constant.MODULES_NAME_DATASTORE_PREFIX
      + collection + COLLECTION_SUFFIX;

    var db = connection.db(undefined).collection(collectionName.toLowerCase());
    db.find({"tf.v.k": {$all: words}}, {fields: {_id: 1}}).toArray(function(err, result) {
      done(err, result);
    });
  };

  async.waterfall([analyze, find], function (err, result) {
    callback(err, result);
  });
};

/**
 * 類似検索の実行 FR対応
 * @param lang
 * @param collection
 * @param out 出力コレクション名
 * @param condition
 * @param text 検索キーワード
 * @param callback
 */
exports.search4 = function (lang, collection, text, condition, callback) {

  if (!text) text = "";

  var tasks = [];

  var collectionName = (constant.MODULES_NAME_DATASTORE_PREFIX
    + collection + COLLECTION_SUFFIX).toLowerCase();

  tasks.push(function (done) {
    // テキスト解析
    var infos = [
      [text, 1]
    ];
    parse(lang, infos, Nbest, function (err, words, v, n) {
      if (err) {
        return  callback(err);
      } else {
        done(err, words, v);
      }
    });
  });

  tasks.push(function (words, v, done) {
    var tf = {w: to_hash_search4(v)};
    done(undefined, words, tf);
  });

  tasks.push(function (words, tf, done) {

    var map = function () {
      util.innerproduct = function (a, v) {
        var sum = 0;
        for (var i = 0, len = a.length; i < len; i++) {
          var info = a[i];
          if (v[info.k]) {
            sum += info.v * v[info.k];
          }
        }
        return sum;
      };
      util.select = function (object, columns) {
        var result = {};
        for (var column in columns) {
          result[column] = object[column];
        }
        return result;
      };

      var obj = util.select(this, this);
      var sum = util.innerproduct(this.tf.v, tf.w);
      if(isNaN(sum)) {
        sum = 0;
      }
      obj["_score"] = sum;
      emit(this._id, obj);
    };

    var reduce = function (key, values) {
      return values[0];
    };

    if (0 < words.length) {
      condition["tf.v.k"] = {$in: words};
    } else {
      return callback(undefined, {});
    }

    var db = connection.db(undefined).collection(collectionName.toLowerCase());
    db.mapReduce(
      map,
      reduce,
      {
        scope: {util: util, sort: {}, tf: tf},
        query: condition,
        out: {inline:1}
      },
      function (err, result) {
        if (err) {
          return done(new errors.InternalServer(err));
        } else {
          return done(err, result);
        }
      }
    );
  });
  async.waterfall(tasks, function (err, result) {
    if (err) {
      log.error(err);
      callback(err);
      return;
    }
    callback(err, result);
  });

};

/**
 * DF計算
 * @param {Object} a
 * @returns {Object} ret
 */
function to_hash_search4(a) {
  var ret = {};
  for (var i = 0, len = a.length; i < len; i++) {
    ret[a[i].k] = 1;
    a[i].w = 1;
  }
  return ret;
}