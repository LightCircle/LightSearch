/**
 * @file 相似度计算
 * @author r2space@gmail.com
 * @module search.searcher
 */

"use strict";

var light       = require("light-framework")
  , async       = light.util.async
  , _           = light.util.underscore
  , errors      = light.framework.error.http
  , log         = light.framework.log
  , connection  = light.framework.mongoconn
  , conf        = light.util.config.app
  , ObjectID    = light.util.mongodb.ObjectID
  , constant    = require("./constant")
  , splitter    = require("./" + (conf.search || "mecab"))
  ;

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

  async.waterfall([totalCount, docCount, idf], function(err, result) {
    callback(err, result);
  });
};


/**
 * 计算相似度
 * 变量说明:
 *  q: 给定文本
 *  d: 数据库文本
 *  numerator = q1.tfidf * d1.tfidf + q2.tfidf * d2.tfidf + ...
 *  denominator = Math.sqrt(q1.tfidf * q1.tfidf + q2.tfidf * q2.tfidf + ...) *
 *                Math.sqrt(d1.tfidf * d1.tfidf + d2.tfidf * d2.tfidf + ...)
 * @param words
 * @param dbwords
 * @returns {number}
 */
function cosine(words, dbwords) {

  _.each(dbwords, function(dbword) {

    var qd = 0, sqrtQ = 0, sqrtD = 0;
    _.each(dbword.idx, function(d, index) {

      sqrtD += d.v.tfidf * d.v.tfidf;
      _.each(words, function(q) {
        if (index === 0) {
          sqrtQ += q * q;
        }
        qd += (q * d.v.tfidf);
      });
    });

    dbword.cosine = qd / Math.sqrt(sqrtQ) * Math.sqrt(sqrtD);
  });

  return dbwords;
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
exports.add = function(code, collection, id, docs, callback) {

  var db = getConnection(code, collection)
    , dbwords = connection.db(code).collection(constant.COLLECTION_WORDS);

  var split = function(done) {
    splitter.parse(docs, function(data) {
      done(undefined, data);
    });
  };

  // 将单词添加到words里，并保存源文档ID
  var updateWords = function(data, done) {
    async.each(data.words, function(item, loop) {
      dbwords.update({ scope: collection, key: item }, { $addToSet: {src: id} }, { upsert: true},
        function(err) {
          loop(err);
        });
    }, function(err) {
      done(err, data);
    });
  };

  // 计算idf
  var calcIdf = function(data, done) {
    exports.idf(code, collection, data.words, function(err, idf) {

      var hash = {};
      _.each(idf, function(item) {
        hash = _.extend(hash, item);
      });

      done(undefined, data, hash);
    });
  };

  // 更新到源数据
  var updateSource = function(data, idf, done) {

    _.each(data.all, function(item) {
      item.v.idf = idf[item.k];
      item.v.tfidf = item.v.tf * item.v.idf;
    });

    db.update({ _id: id }, { $set: {idx: data.all} }, { upsert: true },
      function(err, result) {
        done(err, result);
      });
  };

  async.waterfall([split, updateWords, calcIdf, updateSource], callback);
};

/**
 * 对指定的collection更新分词结果，以id为条件
 * 逻辑为先删除该id在words和制定collection的记录，然后再在指定collection中追加记录
 * @param code
 * @param collection
 * @param id
 * @param docs
 * @param callback
 */
exports.update = function(code, collection, id, docs, callback) {
  var remove = function(done) {
    exports.remove(code, collection, id, done);
  };

  var add = function(done) {
    exports.add(code, collection, id, docs, done);
  };

  async.waterfall([remove, add], callback);
};

/**
 * 删除该id在words和制定collection的记录
 * @param code
 * @param collection
 * @param id
 * @param callback
 */
exports.remove = function(code, collection, id, callback) {

  var db = getConnection(code, collection)
    , dbwords = connection.db(code).collection(constant.COLLECTION_WORDS);

  var removeWords = function(done) {
    dbwords.update({src: {$in: [ObjectID(id)]}}, {$pull:{"src": ObjectID(id)}}, { multi: true }, function(err){
      done(err);
    });
  };

  var removeSource = function(done) {
    db.remove({ _id: ObjectID(id) }, function(err){
      done(err);
    });
  };

  async.waterfall([removeWords, removeSource], callback);
};

// 相似检索
exports.similarSearch = function(code, collection, text, condition, callback) {

  var db = getConnection(code, collection);

  // 分词
  var split = function(done) {
    splitter.parse(text, function(data) {
      done(undefined, data.words);
    });
  };

  // 计算idf
  var calcIdf = function(data, done) {
    exports.idf(code, collection, data, function(err, idf) {

      var hash = {};
      _.each(idf, function(item) {
        hash = _.extend(hash, item);
      });

      done(undefined, data, hash);
    });
  };

  // 获取结果集
  var find = function(words, idf, done) {

    // 根据数据的大小，可以取前10个单词进行匹配
    var or = [];
    _.each(words, function(word) {
      or.push({"idx.k": word});
    });

    var filter = condition || {};
    filter["$or"] = or;

    console.log(filter);
    db.find(filter, { sort: {"idx.v.tf": 1 }, limit: 50}
      , function(err, result) {
        result.toArray(function(err, result) {
          done(err, words, idf, result);
        });
      });
  };

  // 计算相似度(需要遍历所有得结果集)
  var similar = function(words, idf, dbdata, done) {
    done(undefined, words, cosine(idf, dbdata));
  };

  async.waterfall([split, calcIdf, find, similar], function (err, words, result) {
    callback(err, {words: words, id: result});
  });

};

/**
 * 全文检索，对给定的文字进行全文检索
 * text的格式数组[]
 * @param code
 * @param collection
 * @param text
 * @param {Object} condition 附加条件
 * @param callback
 */
exports.fullTextSearch = function(code, collection, text, condition, callback) {

  var db = getConnection(code, collection);

  // 分词
  var split = function(done) {
    splitter.parse(text, function(data) {
      done(undefined, data.words);
    });
  };

  // 用关键字进行检索，获取对象一览的ID
  var find = function(words, done) {
    var filter = condition || {};
    filter["idx.k"] = { $all: words };

    db.find(filter, {fields: ["_id"]}, {sort: {"idx.v.tf": 1}}, function(err, result) {
      result.toArray(function(err, result) {
        done(err, words, result);
      });
    });
  };

  async.waterfall([split, find], function (err, words, result) {
    callback(err, {words: words, id: result});
  });
};

/**
 * 通常检索，没有分词（主要用于无分词工具时，测试开发用）
 * @param code
 * @param collection
 * @param text
 * @param condition
 * @param callback
 */
exports.normalSearch = function(code, collection, text, condition, callback) {

  var db = getConnection(code, collection)
    , filter = condition || {};

  db.find(filter, { fields: ["_id"] }, { sort: {"idx.v.tf": 1} }, function(err, result) {
    result.toArray(function(err, result) {
      callback(err, {words: [text[0][0]], id: result});
    });
  });

};

// TODO: 从指定collection里，获取idx，然后重新生成words表
function remakeWords() {
}

// TODO: 所有数据库值 - 没有必要？
function remakeIdf() {

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

/////////////////////////////////////// 一下未整理
//
//// コレクション拡張子
//var _idf = "_idf"; // 単語辞書テーブル拡張子
//var _similarity = "_similarity"; // 類似度テーブル拡張子
//
//// コレクション(mongooseの複数系対応)
//var COLLECTION_SUFFIX = "s";
//
//
//// Utility
//var util = {
//  // product
//  product: function (a, v) {
//    var b = [];
//    for (var i = 0, len = a.length; i < len; i++) {
//      var ai = a[i];
//      if (ai) {
//        var w = v[ai.k] ? v[ai.k] * ai.v : 0;
//        //b.push({k: ai.k, w: w, r: Object.keys(ai.r)}); 特に読みは必要ない
//        b.push({k: ai.k, w: w});
//      }
//    }
//    return b;
//  },
//  // norm
//  norm: function (a) {
//    var sum = 0;
//    for (var i = 0, len = a.length; i < len; i++) {
//      var w = a[i].w;
//      if (w) {
//        sum += w * w;
//      }
//    }
//    return Math.sqrt(sum);
//  },
//  // innerproduct
//  innerproduct: function (a, v) {
//    var sum = 0;
//    for (var i = 0, len = a.length; i < len; i++) {
//      var info = a[i];
//      if (v[info.k]) {
//        sum += info.w * v[info.k];
//      }
//    }
//    return sum;
//  },
//  // to_hash
//  to_hash: function (a) {
//    var ret = {};
//    for (var i = 0, len = a.length; i < len; i++) {
//      if (a[i].w) {
//        ret[a[i].k] = a[i].w;
//      }
//    }
//    return ret;
//  },
//  // to_array
//  to_array: function (a) {
//    var ret = [a.length];
//    for (var i = 0, len = a.length; i < len; i++) {
//      ret[i] = a[i].k;
//    }
//    return ret;
//  }
//};
//
///**
// *
// * @param collection
// * @param infos {title: 10, createBy: 1}
// * @param callback
// */
//exports.batch = function (lang, collection, index_collection, infos, condition, callback) {
//
//  var tasks = [];
//  // Prepare TF table.
//  tasks.push(function (done) {
//    tf(lang, collection, infos, condition, function (err, table_tfidf) {
//      if (err) {
//        log.error(err);
//        return done(new errors.InternalServer(err));
//      }
//      return done(err, table_tfidf);
//    });
//  });
//  // IDF Dictionary
//  tasks.push(function (table_tfidf, done) {
//    dic(collection, table_tfidf, function (err, table_dic) {
//      if (err) {
//        log.error(err);
//        return done(new errors.InternalServer(err));
//      }
//      return done(err, table_tfidf, table_dic);
//    });
//  });
//  // prepare Dic
//  tasks.push(function (table_tfidf, table_dic, done) {
//    var dic = {};
//    table_dic.find().each(function (err, idf) {
//      if (idf == null) {
//        return done(err, table_tfidf, dic);
//      }
//      dic[idf._id] = idf.value.w;
//    });
//  });
//  // Calc TF-IDF
//  tasks.push(function (table_tfidf, dic, done) {
//    tfidf(table_tfidf, dic, function (err, table_tfidf) {
//      if (err) {
//        log.error(err);
//        return done(new errors.InternalServer(err));
//      }
//      return done(err, table_tfidf);
//    });
//  });
//  // Copy to original collection.
//  tasks.push(function (table_tfidf, done) {
//    table_tfidf.find().each(function (err, tgt) {
//      if (tgt == null) {
//        return done(err, table_tfidf);
//      }
//      index_collection.update({_id: tgt._id}, {$set: {tf: tgt.value}}, {upsert: true}, function (err, result) {
//      });
//    });
//  });
//  // Calcurate Cosine similarity.
//  tasks.push(function (table_tfidf, done) {
//    cosine(collection, table_tfidf, function (err, result) {
//      if (err) {
//        log.error(err);
//        return done(new errors.InternalServer(err));
//      }
//      return done(err, table_tfidf, result);
//    });
//  });
//  async.waterfall(tasks, function (err, table_tfidf, table_similarity) {
//
//    if (err) {
//      log.error(err);
//      callback(err, null);
//      return;
//    }
//
//    log.debug("finished: add message.");
//    callback(err);
//  });
//}
//
/////**
//// *
//// * @param collection
//// * @param infos {title: 10, createBy: 1}
//// * @param callback
//// */
////function tf(lang, collection, infos, condition, callback) {
////
////  var master_name = collection.collectionName;
////  var db = collection.db;
////
////  var target_table = db.collection(master_name + '_tfidf');
////
////  var N = 0;
////  target_table.drop(function (err, reply) {
////    collection.find(condition).each(function (err, src) {
////      if (src != null) {
////        // テキスト解析
////        parse(lang, to_string(src, infos), Nbest, function (err, words, v, n) {
////          ++N;
////          target_table.insert({_id: src._id, value: {v: v, n: n} }, function (err) {
////            if (--N == 0)
////              return callback(err, target_table);
////          });
////
////        });
////      }
////    });
////  });
////}
//
///**
// * IDF Dictionary
// * @param collection
// * @param table_tfidf
// * @param callback
// */
//function dic(collection, table_tfidf, callback) {
//
//  var master_name = collection.collectionName;
//  var db = collection.db;
//
//  var map = function () {
//    var v = this.value.v;
//    if (!v) return;
//    for (var i = 0, len = v.length; i < len; i++) {
//      if (v[i]) {
//        emit(v[i].k, {w: 1, r: v[i].r});
//      }
//    }
//  };
//
//  var reduce = function (key, values) {
//    var total = {w: 0, r: {}};
//    values.forEach(function (value) {
//      total.w += 1;
//      for (var key in value.r) {
//        total.r[key] |= 0;
//        total.r[key] += value.r[key];
//      }
//    });
//    return total;
//  };
//
//  table_tfidf.count(function (err, N) {
//    if (err != null) callback(err);
//    table_tfidf.mapReduce(
//      map,
//      reduce,
//      {
//        scope: {N: N},
//        finalize: function (key, value) {
//          value.w = Math.log(N / value.w);
//          value.r = Object.keys(value.r);
//          return value;
//        },
//        out: {replace: master_name + _idf}
//      },
//      function (err, results) {
//        if (err) {
//          console.log(err);
//          callback(err);
//        }
//        callback(err, results);
//      }
//    );
//  });
//}
//
///**
// *
// * @param collection
// * @param table_tfidf
// * @param dic
// * @param callback
// */
//function tfidf(table_tfidf, dic, callback) {
//
//  var map = function () {
//
//    util.product = function (a, v) {
//      var b = [];
//      for (var i = 0, len = a.length; i < len; i++) {
//        var ai = a[i];
//        if (ai) {
//          var w = v[ai.k] ? v[ai.k] * ai.v : 0;
//          //b.push({k: ai.k, w: w, r: Object.keys(ai.r)}); 特に読みは必要ない
//          b.push({k: ai.k, w: w});
//        }
//      }
//      return b;
//    };
//
//    util.norm = function (a) {
//      var sum = 0;
//      for (var i = 0, len = a.length; i < len; i++) {
//        if (a[i].w) {
//          sum += a[i].w * a[i].w;
//        }
//      }
//      return Math.sqrt(sum);
//    };
//
//    var v = util.product(this.value.v, dic);
//    //var n = this.value.n; // 単語数
//    //emit(this._id, {v: v, n: n, l: util.norm(v)});
//    emit(this._id, {v: v, l: util.norm(v)});
//  };
//
//  var reduce = function (key, values) {
//    return values[0];
//  };
//
//  table_tfidf.mapReduce(
//    map,
//    reduce,
//    {
//      scope: {util: util, dic: dic},
//      out: {replace: table_tfidf.collectionName}
//    },
//    function (err, results) {
//      if (err) {
//        console.log(err);
//        callback(err);
//      }
//      callback(err, results);
//    }
//  );
//
//}
//
///**
// *
// * @param collection
// * @param table_tfidf
// * @param callback
// */
//function cosineold(collection, table_tfidf, callback) {
//
//  var master_name = collection.collectionName;
//  var db = collection.db;
//
//  var target_table = db.collection(master_name + _similarity);
//
//  target_table.drop(function (err, reply) {
//
//    table_tfidf.find().each(function (err, src) {
//
//      if (src == null) {
//        return callback(err, collection);
//      }
//      src.value.condition = util.to_hash(src.value.v);
//      var keys = util.to_array(src.value.v);
//
//      table_tfidf.mapReduce(
//        function () {
//
//          util.innerproduct = function (a, v) {
//            var sum = 0;
//            for (var i = 0, len = a.length; i < len; i++) {
//              var info = a[i];
//              if (v[info.k]) {
//                sum += info.w * v[info.k];
//              }
//            }
//            return sum;
//          };
//
//          var s = util.innerproduct(this.value.v, src.value.condition);
//          if (0 < s) {
//            emit(this._id, s / this.value.l / src.value.l);
//          }
//        },
//        function (key, values) {
//          return values[0];
//        },
//        {
//          scope: {util: util, src: src},
//          query: {_id: {$gt: src._id}, "value.v.k": {$in: keys}},
//          out: {inline: 1}
//        },
//        function (err, results) {
//          if (err) {
//            return callback(err, results);
//          }
//          results.forEach(function (dst) {
//            target_table.insert({a: dst._id, b: src._id, score: dst.value}, function (err, a) {
//            });
//            target_table.insert({a: src._id, b: dst._id, score: dst.value}, function (err, a) {
//            });
//          });
//        }
//      );
//    });
//  });
//}
//
///**
// * tfベクトルvを_idfテーブルを参照してtf-idfベクトルに変換する
// * @param collection
// * @param tf
// * @param callback
// */
//function to_tfidf(code, collection, tf, callback) {
//
//  var words = util.to_array(tf);
//
//  var db = connection.db(code).collection(collection + _idf);
//  db.find({_id: {$in: words}}).toArray(function (err, results) {
//
//    var dic = {};
//    results.forEach(function (tgt) {
//      dic[tgt._id] = tgt.value.w;
//    });
//
//    tf = util.product(tf, dic)
//    callback(err, tf, util.to_hash(tf), util.norm(tf));
//  })
//}
//
///**
// *
// * @param lang 言語
// * @param code
// * @param collection 元データを保存するコレクション
// * @param infos index対象フィールド名{title: 10, createBy: 1}
// * @param object 更新オブジェクト
// * @param callback
// */
//exports.patch = function (lang, code, collection, infos, object, callback) {
//
//  var collectionName = constant.MODULES_NAME_DATASTORE_PREFIX + collection;
//  var indexCollectionName = collectionName + COLLECTION_SUFFIX;
//
//  // テキスト解析
//  parse(lang, to_string(object, infos), Nbest, function (err, words, v, n) {
//
//    to_tfidf(code, collectionName.toLowerCase(), v, function (err, v, w, l) {
//
//      var tf = {v: v, l: l};
//
//      var db = connection.db(code).collection(indexCollectionName.toLowerCase());
//      db.update({_id: object._id}, {$set: {tf: tf}}, {upsert: true}, function (err, result) {
//
//        // TODO: エラーログ追加
//        callback(err, result);
//      });
//    });
//  });
//}
//
///**
// * TFと単語だけを、全文検索用データとして入れる
// * @param lang
// * @param code
// * @param collection
// * @param infos
// * @param object
// * @param callback
// */
//exports.patch1 = function (lang, code, collection, infos, object, callback) {
//
//  var collectionName = constant.MODULES_NAME_DATASTORE_PREFIX + collection;
//  var indexCollectionName = collectionName + COLLECTION_SUFFIX;
//
//  // テキスト解析
//  parse(lang, to_string(object, infos), Nbest, function (err, words, v, n) {
//
//    // TODO: rの値の処理
//    var db = connection.db(code).collection(indexCollectionName.toLowerCase());
//    db.update({_id: object._id}, {$set: {tf: {v: v}}}, {upsert: true}, function (err, result) {
//
//      // TODO: エラーログ追加
//      callback(err, v);
//    });
//  });
//};
//
///**
// * 類似検索の実行
// * @param lang
// * @param collection
// * @param out 出力コレクション名
// * @param condition
// * @param text 検索キーワード
// * @param callback
// */
//exports.search1 = function (lang, collection, index_collection, out, condition, sort, text, callback) {
//
//  if (!text) text = "";
//
//  var tasks = [];
//  tasks.push(function (done) {
//    // テキスト解析
//    var infos = [
//      [text, 1]
//    ];
//    parse(lang, infos, Nbest, function (err, words, v, n) {
//      if (err) {
//        return  callback(err, collection.db.collection(out));
//      } else {
//        done(err, words, v);
//      }
//    });
//  });
//  tasks.push(function (words, v, done) {
//    to_tfidf(collection, v, function (err, v, w, l) {
//      if (err) {
//        return  callback(err, collection.db.collection(out));
//      } else {
//        var tf = {w: w, l: l};
//        done(err, words, tf);
//      }
//    });
//  });
//  tasks.push(function (words, tf, done) {
//
//    var map = function () {
//      util.innerproduct = function (a, v) {
//        var sum = 0;
//        for (var i = 0, len = a.length; i < len; i++) {
//          var info = a[i];
//          if (v[info.k]) {
//            sum += info.w * v[info.k];
//          }
//        }
//        return sum;
//      };
//      util.select = function (object, columns) {
//        var result = {};
//        columns.forEach(function (column) {
//          result[column] = object[column];
//        });
//        return result;
//      };
//      var obj = util.select(this, sort);
//      var sum = util.innerproduct(this.tf.v, tf.w);
//      obj["_score"] = sum / this.tf.l / tf.l;
//      emit(this._id, obj);
//    };
//
//    var reduce = function (key, values) {
//      return values[0];
//    };
//
//    if (0 < words.length) {
//      condition["tf.v.k"] = {$in: words};
//    }
//
//    index_collection.mapReduce(
//      map,
//      reduce,
//      {
//        scope: {util: util, sort: sort, tf: tf},
//        query: condition,
//        out: out
//      },
//      function (err, result) {
//        if (err) {
//          return done(new errors.InternalServer(err));
//        } else {
//          return done(err, result);
//        }
//      }
//    );
//  });
//  async.waterfall(tasks, function (err, result) {
//    if (err) {
//      log.error(err);
//      callback(err);
//      return;
//    }
//    callback(err, result);
//  });
//
//}
//
///**
// * 完全一致検索用の条件生成
// * @param text
// * @param condition
// * @param callback
// */
//exports.condition = function (lang, text, callback) {
//  var nbest = 1; // 完全一致なので形態素解析の結果は最良を1つだけ使用する
//  var infos = [
//    [text, 1]
//  ];
//  parse(lang, infos, nbest, function (err, words, tf, i) {
//    callback(null, words, i);
//  });
//}
//
///**
// * カラム名から実際の文字列に変換する
// * @param object
// * @param infos {title: 10, createBy: 1}
// * @returns {Array} [ [ "hogehoge", 10], [ "....", 1 ] ]
// */
//function to_string(object, infos) {
//
//  var results = [];
//  for (var key in infos) {
//    results.push([object._doc[key], infos[key]]);
//  }
//  return results;
//}
//
///**
// * 通常検索
// * @param lang
// * @param collection
// * @param out 出力コレクション名
// * @param condition
// * @param sort ソートに使用するカラムの配列
// * @param callback
// */
//exports.search0 = function (lang, collection, index_collection, out, condition, sort, text, callback) {
//
//  if (!text) text = "";
//
//  var tasks = [];
//  tasks.push(function (done) {
//    // テキスト解析
//    var nbest = 1; // 完全一致なので形態素解析の結果は最良を1つだけ使用する
//    var infos = [
//      [text, 1]
//    ];
//    parse(lang, infos, nbest, function (err, words, v, n) {
//      if (err) {
//
//      } else {
//        done(err, words);
//      }
//    });
//  });
//  tasks.push(function (words, done) {
//
//    var map = function () {
//      util.select = function (object, columns) {
//        var result = {};
//        columns.forEach(function (column) {
//          result[column] = object[column];
//        });
//        return result;
//      };
//      var obj = util.select(this, sort);
//      emit(this._id, obj);
//    };
//
//    var reduce = function (key, values) {
//      return values[0];
//    };
//
//    if (0 < words.length) {
//      condition["tf.v.k"] = {$all: words};
//    }
//
//    var db = connection.db(undefined).collection(index_collection);
//    db.mapReduce(
//      map,
//      reduce,
//      {
//        scope: {util: util, sort: sort},
//        query: condition,
//        out: out
//      },
//      function (err, result) {
//        if (err) {
//          return done(new errors.InternalServer(err));
//        } else {
//          return done(err, result);
//        }
//      }
//    );
//  });
//  async.waterfall(tasks, function (err, result) {
//    if (err) {
//      log.error(err);
//      callback(err);
//      return;
//    }
//    callback(err, result);
//  });
//};
//
///**
// * シンプルサーチ
// * @param lang
// * @param code
// * @param collection
// * @param text
// * @param callback
// */
//exports.search3 = function (lang, code, collection, text, callback) {
//
//  // テキスト解析
//  var analyze = function (done) {
//    parse(lang, [text || "", 1], 1, function (err, words) {
//      done(err, words);
//    });
//  };
//
//  // キーワードで検索、対象一覧のIDを返す
//  var find = function (words, done) {
//
//    var collectionName = constant.MODULES_NAME_DATASTORE_PREFIX
//      + collection + COLLECTION_SUFFIX;
//
//    var db = connection.db(undefined).collection(collectionName.toLowerCase());
//    db.find({"tf.v.k": {$all: words}}, {fields: {_id: 1}}).toArray(function(err, result) {
//      done(err, result);
//    });
//  };
//
//  async.waterfall([analyze, find], function (err, result) {
//    callback(err, result);
//  });
//};
//
///**
// * 類似検索の実行 FR対応
// * @param lang
// * @param collection
// * @param out 出力コレクション名
// * @param condition
// * @param text 検索キーワード
// * @param callback
// */
//exports.search4 = function (lang, collection, text, condition, callback) {
//
//  if (!text) text = "";
//
//  var tasks = [];
//
//  var collectionName = (constant.MODULES_NAME_DATASTORE_PREFIX
//    + collection + COLLECTION_SUFFIX).toLowerCase();
//
//  tasks.push(function (done) {
//    // テキスト解析
//    var infos = [
//      [text, 1]
//    ];
//    parse(lang, infos, Nbest, function (err, words, v, n) {
//      if (err) {
//        return  callback(err);
//      } else {
//        done(err, words, v);
//      }
//    });
//  });
//
//  tasks.push(function (words, v, done) {
//    var tf = {w: to_hash_search4(v)};
//    done(undefined, words, tf);
//  });
//
//  tasks.push(function (words, tf, done) {
//
//    var map = function () {
//      util.innerproduct = function (a, v) {
//        var sum = 0;
//        for (var i = 0, len = a.length; i < len; i++) {
//          var info = a[i];
//          if (v[info.k]) {
//            sum += info.v * v[info.k];
//          }
//        }
//        return sum;
//      };
//      util.select = function (object, columns) {
//        var result = {};
//        for (var column in columns) {
//          result[column] = object[column];
//        }
//        return result;
//      };
//
//      var obj = util.select(this, this);
//      var sum = util.innerproduct(this.tf.v, tf.w);
//      if(isNaN(sum)) {
//        sum = 0;
//      }
//      obj["_score"] = sum;
//      emit(this._id, obj);
//    };
//
//    var reduce = function (key, values) {
//      return values[0];
//    };
//
//    if (0 < words.length) {
//      condition["tf.v.k"] = {$in: words};
//    } else {
//      return callback(undefined, {});
//    }
//
//    var db = connection.db(undefined).collection(collectionName.toLowerCase());
//    db.mapReduce(
//      map,
//      reduce,
//      {
//        scope: {util: util, sort: {}, tf: tf},
//        query: condition,
//        out: {inline:1}
//      },
//      function (err, result) {
//        if (err) {
//          return done(new errors.InternalServer(err));
//        } else {
//          return done(err, result);
//        }
//      }
//    );
//  });
//  async.waterfall(tasks, function (err, result) {
//    if (err) {
//      log.error(err);
//      callback(err);
//      return;
//    }
//    callback(err, result);
//  });
//
//};
//
///**
// * DF計算
// * @param {Object} a
// * @returns {Object} ret
// */
//function to_hash_search4(a) {
//  var ret = {};
//  for (var i = 0, len = a.length; i < len; i++) {
//    ret[a[i].k] = 1;
//    a[i].w = 1;
//  }
//  return ret;
//}