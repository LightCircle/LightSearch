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

var light       = require("light-framework")
  , async       = light.util.async
  , _           = light.util.underscore
  , errors      = light.framework.error.http
  , log         = light.framework.log
  , path        = light.lang.path
  , fs          = light.lang.fs
  , MeCab       = require("mecab-async")
  , connection  = light.framework.mongoconn
  , constant    = require("./constant")
  , conf        = light.util.config.app

  , ObjectID     = light.util.mongodb.ObjectID
  ;

// Node shims for < v0.7
fs.existsSync = fs.existsSync || path.existsSync;

// 類似検索のインデックスとしてはヒットの可能性を広げるためにnbest=2とする
var Nbest = 1;

// コレクション拡張子
var _idf = "_idf"; // 単語辞書テーブル拡張子
var _similarity = "_similarity"; // 類似度テーブル拡張子

// コレクション(mongooseの複数系対応)
var COLLECTION_SUFFIX = "s";


// pos : parts of speech (品詞)
var pos = {
  "名詞": {"一般": 1, "固有名詞": 1, "数": 1, "サ変接続": 1, "形容動詞語幹": 1, "副詞可能": 1},
  "動詞": {"自立": 1},
  "形容詞": {"自立": 1}
};

// pm : punctuation marks (句読点) 句読点で区切って形態素解析する
//var _pm = /[。、]/;
//var _pm = /[-]/;


var option = {
  "nbest": Nbest,
  "dicdir": "/Users/lilin/developer/light/LightBin/dic/ja",
  "userdic": "/Users/lilin/developer/light/LightBin/dic/ja/user.dic"
};


/**
 * 分词，并计算TF值
 * @param docs [["文档1", 权重1], ["文档2", 权重2]]
 */
function parse(docs) {

  var mecab = new MeCab()
    , terms = {}  // 分词结果
    , count = 0;  // 词的总个数

  _.each(docs, function(info) {

    var text = toLower(info[0] || ""), weight = info[1];
    _.each(mecab.parseSync(text), function (term) {

      // kanji         : 0 词
      // lexical       : 1 词性
      // compound      : 2 组成（自立，非自立，...）
      // compound2     : 3 组成
      // compound3     : 4 组成
      // conjugation   : 5 活用
      // inflection    : 6 なまり
      // original      : 7 原型
      // reading       : 8 片假名读音
      // pronunciation : 9 发音
      var kanji = term[0], lexical = term[1], compound = term[2], original = term[7], reading = term[8];

      // 根据词性过滤单词
      if (pos[lexical] && pos[lexical][compound]) {

        kanji = (original === "*") ? kanji : original;    // 如果有原型，则使用原型
        reading = (reading === "*") ? kanji : reading;    // 保存读音

        // k 汉字
        // r 读音
        // v {
        //   w 权重
        //   n 单词在给定句子中出现的次数
        //   cnt 给定句子的单词数
        //   tf tf值
        //   idf idf值
        //   tfidf tf乘idf的值
        // }
        terms[kanji] = terms[kanji] || { k: '', r: '', v: {w: weight, n: 0, tf: 0, idf: 0, tfidf: 0} };
        terms[kanji].k = kanji;
        terms[kanji].r = reading;
        terms[kanji].v.n += weight;
        count += weight;
      }
    });
  });

  var result = [];
  _.each(terms, function(val, key) {
    val.v.tf = val.v.n / count;
    result.push(val);
  });

  return { words: _.keys(terms), all: result, count: count };
}
exports.parse = parse;


/**
 * 英数转换成半角 + 变成小写
 * @param text
 * @returns {XML|string}
 */
function toLower(text) {

  // 英数转换成半角
  text = text.replace(/[Ａ-Ｚａ-ｚ０-９]/g, function(str) {
    return String.fromCharCode(str.charCodeAt(0) - 0xFEE0);
  });

  // 变成小写
  text = text.replace(/[A-Z]/g, function(str) {
    return String.fromCharCode(str.charCodeAt(0) + 0x20);
  });

  return text;
}


function remakeWords() {

  // 从指定collection里，获取idx，然后重新生成words表
}

// 所有数据库值
function reflashIdf() {

//  var total = 1000; // 总文档数;
//
//  var word = []; // 所有得单词;
//  // mongo distinct
//
//  // 循环word
//
//    var count = 10;// word 所在的文档个数
//    // mongo count + condition
//
//    var idf = Math.log(total / count);
//
//    tfidf = tf * idf;
//    // 更新数据
}

/**
 * 给定单词数组，计算每个单词的idf值
 * @param code
 * @param collection
 * @param words
 * @param callback
 */
exports.idf = function(code, collection, words, callback) {

  var db = getConnection(code, collection)
    , dbwords = connection.db(code).collection(constant.COLLECTION_WORDS);

  // 获取总文档书
  var totalCount = function(done) {
    db.count(function(err, total) {
      done(err, total);
    });
  };

  // 获取包含指定词的文档个数
  var docCount = function(total, done) {
    dbwords.find({ scope: collection, key: {$in: words} }, function(err, result) {
      result.toArray(function(err, result) {
        done(err, total, result);
      });
    });
  };

  // 计算idf值 = Math.log(total / count)
  var idf = function(total, docs, done) {
    var result = [];
    _.each(docs, function(doc) {
      var item = {};
      item[doc.key] = Math.log(total / doc.src.length);
      result.push(item);
    });

    done(null, result);
  };

  async.waterfall([totalCount, docCount, idf], callback);
}


// 计算相似度
function cosine(words) {


//  // 循环words，包含tfidf
//  [{word1: w_tfidf1}, {word2: w_tfidf2}]
//
//  // 与数据库的每条计算
//  [{word1: d_tfidf1}, {word2: d_tfidf2}, {data3: d_tfidf3}]
//
//
//  numerator = w_tfidf1 * d_tfidf1 + w_tfidf2 * d_tfidf2;
//
//
//  denominator = Math.sqrt(w_tfidf1 * w_tfidf1 + w_tfidf2 * w_tfidf2) *
//    Math.sqrt(d_tfidf1 * d_tfidf1 + d_tfidf2 * d_tfidf2 + d_tfidf3 * d_tfidf3)
//
//
//  reutrn numerator / denominator;
}

/**
 * 获取连接
 * @param code
 * @param collection
 * @returns {*}
 */
function getConnection(code, collection) {
  return connection.db(code).collection((constant.COLLECTION_PREFIX + collection + "s").toLowerCase());
}

/**
 * 向指定的collection插入分词结果，以id为条件
 * 内包含大量数据库操作。比如像添加文件这样的大量词的添加，需要MQ机制来执行。
 * @param code
 * @param collection
 * @param id
 * @param docs
 * @param callback
 */
exports.append = function(code, collection, id, docs, callback) {

  var data = parse(docs) // 分词
    , db = getConnection(code, collection)
    , dbwords = connection.db(code).collection(constant.COLLECTION_WORDS);

  // 将单词添加到words里，并保存源文档ID
  var updateWords = function(done) {
    async.each(data.words, function(item, loop) {
      dbwords.update({ scope: collection, key: item }, { $addToSet: {src: id} }, { upsert: true},
        function(err, result) {
          loop(err);
        });
    }, function(err, result) {
      done(err, result);
    });
  };

  // 计算idf
  var calcIdf = function(done) {
    idf(code, collection, data.words, done);
  };

  // 更新到源数据
  var updateSource = function(idf, done) {

    // TODO:set idf & tfidf

    db.update({ _id: ObjectID(id) }, { $set: {idx: data.all} }, { upsert: true },
      function(err, result) {
        done(err, result);
      });
  };

  async.waterfall([updateWords, calcIdf, updateSource], callback);
};

exports.update = function() {

}

exports.remove = function() {

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

///**
// *
// * @param collection
// * @param infos {title: 10, createBy: 1}
// * @param callback
// */
//function tf(lang, collection, infos, condition, callback) {
//
//  var master_name = collection.collectionName;
//  var db = collection.db;
//
//  var target_table = db.collection(master_name + '_tfidf');
//
//  var N = 0;
//  target_table.drop(function (err, reply) {
//    collection.find(condition).each(function (err, src) {
//      if (src != null) {
//        // テキスト解析
//        parse(lang, to_string(src, infos), Nbest, function (err, words, v, n) {
//          ++N;
//          target_table.insert({_id: src._id, value: {v: v, n: n} }, function (err) {
//            if (--N == 0)
//              return callback(err, target_table);
//          });
//
//        });
//      }
//    });
//  });
//}

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
      callback(err, v);
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