

var s = require("./lib/searcher")
  , m = require("./lib/mecab");


var res = m.parse([["インデックス一括,ＡdA作成する.sadfとき、十分に注意する必要のある作成機能", 10], ["李林", 1]]);

console.log(res);


//s.append("LawyerOnline", "test"
//  , "539ebb9c0856a8b40639caee"
//  , [["インデックス一括,ＡdA作成する.sadfとき、十分に注意する必要のある作成機能", 10], ["李林", 1]]
//  , function(err, res1) {
//
//    console.log(err);
//    console.log(res1);
//
//    process.exit(0);
//  });
//
//s.idf("LawyerOnline", "test", ["一括", "sadf"], function(err, res1) {
//  console.log(err);
//  console.log(res1);
//  process.exit(0);
//});
