var mmseg = require('./lib/mmseg');

//var s = require("./lib/searcher")
//  , m = require("./lib/mecab");

var txt = "施乃康说，我以我的视觉与感悟，告诉来与我聊天的华人华侨和海外施氏宗亲们，告诉他们为了中华民族的复兴和中国人在世界上有尊严，习近平和他的同事们工作的很累、很辛苦。我们必须坚定的支持习近平和他的同事们，支持中国共产党一心为民族复兴的事业。";
//var res = m.parse([["インデックス一括,ＡdA作成する.sadfとき、十分に注意する必要のある作成機能", 10], ["李林", 1]]);

console.log(mmseg.segStr(txt));


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