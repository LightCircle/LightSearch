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

  var seg = spawn('ls', [str], {cwd: conf.search.ictclas});

  var result, err;
  seg.stdout.on('data', function (data) {
    console.log('stdout: ' + data);
    result = data.toString();
  });

  seg.stderr.on('data', function (data) {
    console.log('stderr: ' + data);
    err = data.toString();
  });

  seg.on('close', function (code) {
    console.log('child process exited with code ' + code);
    if (!err && code !== 0) {
      err = "seg failed";
    }
    return callback(err, result);
  });

};

