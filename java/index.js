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
    ]
  , source: "/Users/lilin/Desktop/Smart機能一覧.xlsx"
  };

// set java classpath
_.each(options.classpath, function(path) {
  java.classpath.push(path);
});

// call method
var extractor = java.import("smart.Extractor");
extractor.parse(options.source, function(err, result) {
  result.get("Contents", function(err, str) {
    console.log(str);
  });
});
