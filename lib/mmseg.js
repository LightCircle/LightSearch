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
  , log         = light.framework.log
  , os          = light.lang.os
  , util        = light.lang.util
  , conf        = light.util.config
  , exec        = require("child_process").exec
  ;

var Blacklist = {};

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

    var cmd = util.format('%s -d %s %s', conf.search.binPath, conf.search.dicPath, path);
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

    words = _.filter(words, function (word) {
      return !_.has(Blacklist, word);
    })
    done(null, words);

  }

  async.waterfall([ gerFilename, writeFile , doSeg, trimWords, filterWords], function (err, result) {

    return callback(err, result);

  });

};

exports.loadBlacklist = function (callback) {

  fs.readFile(conf.search.blacklist, {encoding: 'utf8'}, function (err, data) {
    if (err) {
      callback(err)
    }
    var words = _.str.lines(data.toString());
    words.sort();
    _.each(words, function (word) {
      Blacklist[word] = 1;
    });
    callback(Blacklist);
  });
};

exports.flushBlacklist = function(callback){

  var stream = fs.createWriteStream(conf.search.blacklist, {encoding: 'utf8'});
  stream
    .on('error', function (err) {
      return callback(err);
    })
    .on('close', function () {
      return(null, Blacklist);
    });

  _.each(_.keys(Blacklist),function(word){
    stream.write(word + "\n",'utf8');
  });
  stream.end();

};

exports.extendBlacklist = function (words, callback) {

  _.each(words, function (word) {
    Blacklist[word] = 1;
  });
  exports.flushBlacklist(callback);

}

exports.extendDic = function (words, callback) {
  var dicFile = path.join(conf.search.dicPath, "unigram.txt");
  var dicTmp = path.join(conf.search.dicPath, "unigram.txt.uni");
  var dicLib = path.join(conf.search.dicPath, "uni.lib");
  var content = "";
  _.each(words, function (word) {
    content += word + "\t1\n" + "x:1\n";
  });
  fs.appendFile(dicFile, content, function (err) {
    if (err) callback(err);

    var cmd = util.format('%s -u %s && mv -f %s %s && chmod +x %s', conf.search.binPath, dicFile, dicTmp, dicLib, dicLib);

    exec(cmd, function (err, stdout, stderr) {
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


