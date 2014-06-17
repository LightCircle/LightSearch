/**
 * @file mmseg 中文分词
 * @author 罗浩
 * @module search.searcher
 */

"use strict";

var light       = require("light-framework")
  , async       = light.util.async
  , _           = light.util.underscore
  , path        = light.lang.path
  , fs          = light.lang.fs
  , os          = light.lang.os
  , exec        = require("child_process").exec
  ;

exports.segStr = function (str, callback) {

  var gerFilename = function (done) {

    var name = process.pid + '-' + (Math.random() * 0x100000000 + 1).toString(36) + '.txt';
    done(null, name);

  }

  var writeFile = function (filename, done) {

    var filepath = path.join(os.tmpdir(), filename);
    fs.writeFile(filepath, str, function (err) {
      done(err, filepath);
    });

  }

  var doSeg = function (path, done) {

    var cmd = "/usr/local/mmseg3/bin/mmseg -d /usr/local/mmseg3/etc " + path;
    exec(cmd, function (err, stdout) {
      fs.unlinkSync(path);
      done(err, stdout ? stdout.toString() : null);
    });

  }

  var trimWords = function (content, done) {

    var words = [];
    var lines = _.str.lines(content);
    for (var i = 0; i < lines.length - 2; i++) {
      _.each(_.words(lines[i], '/x '), function (word) {
        words.push(word);
      });
    }
    done(null, words);

  }

  var filterWords = function (words, done) {

    done(null, words);

  }

  async.waterfall([ gerFilename, writeFile , doSeg, trimWords, filterWords], function (err, result) {

    return callback(err, result);

  });

};