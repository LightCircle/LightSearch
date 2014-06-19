/**
 * @file ICTCLAS 中文分词
 * @author 罗浩
 * @module search.searcher
 */

"use strict";

var light       = require("light-framework")
  , async       = light.util.async
  , _           = light.util.underscore
  , path        = light.lang.path
  , fs          = light.lang.fs
  , log         = light.framework.log
  , os          = light.lang.os
  , util        = light.lang.util
  , conf        = light.util.config
  , exec        = require("child_process").exec
  , spawn       = require("child_process").spawn
  ;

/**
 * @desc 分词方法
 * @param {String} 需要分词的字符串
 * @param {Function} callback 回调函数，返回分完词的数组
 */
exports.segStr = function (str, callback) {

  var seg = spawn(path.join(conf.search.ictclas, 'seg'), [str], {cwd: conf.search.ictclas});

  var result, err;
  seg.stdout.on('data', function (data) {
    result = data.toString();
  });

  seg.stderr.on('data', function (data) {
    err = data.toString();
  });

  seg.on('close', function (code) {
    if (!err && code !== 0) {
      err = "seg failed";
    }
    return callback(err, result);
  });
};
/**
 * @desc 扩展分词词典.
 * @param {Array} 要扩展的词
 * @param {Function} callback 失败返回 err,返回 null
 */
exports.extendDic = function (words, callback) {

  var dicFile = path.join(conf.search.ictclas, "userdic.txt");
  var content = "";
  _.each(words, function (word) {
    content += word + "@@g\n";
  });

  fs.appendFile(dicFile, content, function (err) {
    if (err) callback(err);

    var cmd = util.format('%s %s', path.join(conf.search.ictclas, 'dic'), dicFile);

    exec(cmd, {cwd: conf.search.ictclas}, function (err, stdout, stderr) {
      if (err) {
        callback(err);
      }
      if (stderr) {
        log.error(stderr.toString());
      }
      callback(null);
    });
  });
}

