
"use strict";

var light       = require("light-framework")
  , async       = light.util.async
  , errors      = light.framework.error.http
  , log         = light.framework.log
  , path        = light.lang.path
  , fs          = light.lang.fs
  , exec        = require("child_process").exec;


var dicdir = "/Users/lilin/developer/light/LightBin/dic";


/**
 * 生成用户词典
 * @param code
 * @param lang
 * @param vs
 * @param callback
 */
exports.makeDictionary = function (lang, words, callback) {

  var createCsv = function (done) {

    // 用户字典文件（csv格式，最终用mecab-dict-index命令，生成dic文件）
    var file = path.resolve(path.join(dicdir, lang, 'user.csv'));

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
      stream.write(","); // 左文脈ID
      stream.write(","); // 右文脈ID
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
