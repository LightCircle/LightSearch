/**
 * @file java调用器
 * @author r2space@gmail.com
 */

"use strict";

var light = require("light-framework")
  , _     = light.util.underscore
  , java  = require("java");

var options = {
  classpath: [
      "./target/classes"
    , "../../LightBin/tika-app-1.5.jar"
    , "../../LightBin/ansj_seg-1.4.1.jar"
    , "../../LightBin/tree_split-1.4.jar"
    ]
  , source: "/Users/lilin/Desktop/Smart機能一覧.xlsx"
  };

// set java classpath
_.each(options.classpath, function(path) {
  java.classpath.push(path);
});

// call method
var extractor = java.import("light.Ansj");
extractor.parse("此次更新，对tree-split中潜伏了n久的偏移量错误进行了修正。", function(err, result) {

  var length = result.sizeSync();

  result.get(0, function(err, res) {
    console.log(res);
  });
});
