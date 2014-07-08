/**
 * @file ICTCLAS 中文分词
 * @author r2space
 * @module search.searcher
 */

"use strict";

var light       = require("light-framework")
  , async       = light.util.async
  , _           = light.util.underscore
  , csegment    = require("./csegment")
  ;

var pos = {
    n: 1 // 名词
  , a: 1 // 形容词
  , v: 1 // 动词
};

/**
 * 分词，并计算TF值
 * @param docs [["文档1", 权重1], ["文档2", 权重2]]
 * @param callback
 */
exports.parse = function(docs, callback) {

  var terms = {}  // 分词结果
    , count = 0;  // 词的总个数

  async.each(docs, function(info, loop) {
    var text = toLower(info[0] || ""), weight = info[1];
    csegment.doWork(text, function (term) {

      var words = term.split(" ");
      _.each(words, function(term) {
        var n = term.lastIndexOf("/")
          , hanzi = term.substring(0, n)
          , lexical = term.substr(n + 1, 1);

        // 变量说明:
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
        if (pos[lexical]) {
          terms[hanzi] = terms[hanzi] || { k: '', v: {w: weight, n: 0, tf: 0, idf: 0, tfidf: 0} };
          terms[hanzi].k = hanzi;
          terms[hanzi].v.n += weight;
          count += weight;
        }
      });
      loop();
    });
  }, function() {

    var result = [];
    _.each(terms, function(val) {
      val.v.tf = val.v.n / parseFloat(count);
      result.push(val);
    });

    callback({ words: _.keys(terms), all: result, count: count });
  });
};

/**
 * 英数转换成半角 + 变成小写
 * TODO: 移到framework helper里
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