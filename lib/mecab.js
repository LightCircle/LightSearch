/**
 * @file MeCab日语分词
 * @author r2space@gmail.com
 * @module search.searcher
 */

"use strict";

var light       = require("light-framework")
  , async       = light.util.async
  , _           = light.util.underscore
  , errors      = light.framework.error.http
  , log         = light.framework.log
  , path        = light.lang.path
  , fs          = light.lang.fs
  , exec        = require("child_process").exec
  , MeCab       = require("mecab-async")
  ;


// pos : parts of speech (品詞)
var pos = {
  "名詞": {"一般": 1, "固有名詞": 1, "数": 1, "サ変接続": 1, "形容動詞語幹": 1, "副詞可能": 1},
  "動詞": {"自立": 1},
  "形容詞": {"自立": 1}
};

var option = {
  "nbest": Nbest,
  "dicdir": "/Users/lilin/developer/light/LightBin/dic/ja",
  "userdic": "/Users/lilin/developer/light/LightBin/dic/ja/user.dic"
};

// 分词精度
var Nbest = 1;

/**
 * 分词，并计算TF值
 * @param docs [["文档1", 权重1], ["文档2", 权重2]]
 */
exports.parse = function(docs) {

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


/**
 * TODO：生成用户词典
 * @param code
 * @param lang
 * @param vs
 * @param callback
 */
exports.makeDictionary = function (lang, words, callback) {

  var createCsv = function (done) {

    // 用户字典文件（csv格式，最终用mecab-dict-index命令，生成dic文件）
    var file = path.resolve(path.join(option.dicdir, lang, 'user.csv'));

    var stream = fs.createWriteStream(file);
    stream
      .on('error', function (err) {
        return callback(new errors.InternalServer(err));
      })
      .on('close', function () {
        done(null, file);
      });

    // 按指定格式写入
    words.forEach(function (w) {
      stream.write(w.title + ",");
      stream.write(",");
      stream.write(",");
      stream.write((w.cost || 0) + ",");
      stream.write((w.lexical || "*") + ",");
      stream.write((w.compound1 || "*") + ",");
      stream.write((w.compound2 || "*") + ",");
      stream.write((w.compound3 || "*") + ",");
      stream.write((w.conjugation || "*") + ",");
      stream.write((w.inflection || "*") + ",");
      stream.write((w.original || "*") + ",");
      stream.write((w.reading || "*") + ",");
      stream.write((w.pronunciation || "*") + "\n");
    });
    stream.end();
  };

  var createDic = function (file, done) {

    // 命令行参数
    var options = {
      "dicdir": path.resolve(path.join(dicdir, lang)),
      "charset": "utf8",
      "dictionary-charset": "utf8",
      "userdic": path.resolve(path.join(dicdir, lang, "user.dic"))
    };

    var param = "";
    for (var key in options) {
      param += ' --' + key + '="' + options[key] + '"';
    }
    return param;

    exec("mecab-dict-index" + param + " " + file, function (err) {
      return done(null);
    });
  };

  async.waterfall([createCsv, createDic], function (err) {
    log.debug("finished: makeDictionary.");
    callback(err);
  });
}