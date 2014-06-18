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

/**
 * @desc 分词方法
 * @param {String} 需要分词的字符串
 * @param {Function} callback 回调函数，返回分完词的数组
 */
exports.segStr = function (str, callback) {

  // step1.获取一个随机的文件名
  var gerFilename = function (done) {

    var name = process.pid + '-' + (Math.random() * 0x100000000 + 1).toString(36) + '.txt';
    done(null, name);

  }

  // step2.将内容写入文件
  var writeFile = function (filename, done) {

    var filepath = path.join(os.tmpdir(), filename);
    fs.writeFile(filepath, str, function (err) {
      done(err, filepath);
    });

  }

  // step3.执行分词命令
  var doSeg = function (path, done) {

    var cmd = util.format('%s -d %s %s', conf.search.binPath, conf.search.dicPath, path);
    exec(cmd, function (err, stdout) {
      fs.unlinkSync(path);
      done(err, stdout ? stdout.toString() : null);
    });

  }

  // step4.加工分词结果,去除不必要信息
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

  // step5.过滤要排除的词
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


/**
 * @desc 加载需要过滤的词.
 *       过滤词文件为 utf-8 编码的文本文件,每一行一个词,路径配置到 conf.search.blacklist
 * @param {Function} callback 回调函数，返回加载后的过滤词
 */
exports.loadBlacklist = function (callback) {

  if(!fs.existsSync(conf.search.blacklist)){
    return callback(null,{});

  }

  fs.readFile(conf.search.blacklist, {encoding: 'utf8'}, function (err, data) {
    if (err) {
      callback(err)
    }
    var words = _.str.lines(data.toString());
    words.sort();
    _.each(words, function (word) {
      Blacklist[word] = 1;
    });
    callback(err, Blacklist);
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

/**
 * @desc 扩展过滤词列表.
 * @param {Array} 要扩展的过滤词
 * @param {Function} callback 回调函数，返回扩展后的过滤词
 */
exports.extendBlacklist = function (words, callback) {

  _.each(words, function (word) {
    Blacklist[word] = 1;
  });
  exports.flushBlacklist(callback);

}

/**
 * @desc 扩展分词词典.
 * @param {Array} 要扩展的词
 * @param {Function} callback 失败返回 err,返回 null
 */
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


