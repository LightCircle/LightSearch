//var ictclas = require('./lib/csegment');
//var ictclas = require('./lib/ictclas');
//var s = require("./lib/searcher")
//  , m = require("./lib/mecab");
//console.log(__dirname);
//var txt = "施乃康说，（一）我以我的视觉与感悟，（二）告诉来与我聊天的华人华侨和海外施氏宗亲们，（三）告诉他们为了中华民族的复兴和中国人在世界上有尊严，习近平和他的同事们工作的很累、很辛苦。我们必须坚定的支持习近平和他的同事们，支持中国共产党一心为民族复兴的事业。";
//ictclas.segStr(txt, function (err, result) {
//  console.log(result);
//});
//var res = m.parse([["インデックス一括,ＡdA作成する.sadfとき、十分に注意する必要のある作成機能", 10], ["李林", 1]]);


/*
* !!!目前ictclas只支持64位 linux 系统,其他系统均不支持!!!
* */

 /*
* doExtend
* 用来扩展分词词典
* 参数:
*   为自定义词典的绝对路径,自定义词典里的词词性识别码为g.
* 返回值:
*   加载的自定义词的个数(由于对 C++ 不熟,所以返回值是字符串类型的数字.)
* 注意点:
*   1.可能是由于 ictclas 的 bug,自定义词典文件的第一行无法被识别,所以第一行需要留空.
*   2.除第一行以外,其他行每一行写一个词,格式为 "词语@@g".
*   3.自定义词典的导入是全局的.比如说第一次导入 A B 两个词,第二次导入只导入 A 词,
*     则第二次导入后分词系统不会识别出 B 这个词.
*
* */

/*
 * doWork
 * 用来分词
 * 参数:
 *   需要分词的内容,必须为 UTF-8 编码.
 * 返回值:
 *   未切分的分词结果.
 *
 * */

// ictclas.doExtend('/opt/LightSearch/ictclas/userdic.txt', function (result) {
//  console.log(result);
//  ictclas.doWork(txt, function (result) {
//    console.log(result);
//  });
//});
//ictclas.doWork(txt,function(result){
//    console.log(result);
//});

//mmseg.loadBlacklist(function (err, blacklist) {
//  console.log(blacklist);
//  mmseg.segStr(txt, function (err, result) {
//    console.log(result);
//    console.log("-------------------------------------");
//    mmseg.extendBlacklist(['感悟'], function (err, blacklist) {
//      mmseg.segStr(txt, function (err, result) {
//        console.log(result);
//      });
//    });
//  });
//});
//mmseg.segStr(txt, function (err, result) {
//  console.log(result);
//  console.log("-------------------------------------");
//  mmseg.extendDic(['（一）', '（二）'], function (err) {
//    if (err) console.log(err);
//    mmseg.segStr(txt, function (err, result) {
//      console.log(result);
//    });
//  });
//});


var searcher = require("./lib/searcher");
console.time("searcher");

//function testSearch() {
//  searcher.add("LawyerOnline", "test", "539ebb9c0856a8b40639caee"
//    , [["インデックス一括,ＡdA作成する.sadfとき、十分に注意する必要のある十分作成機能", 10], ["李林", 1]]
//    , function(err, res1) {
//
//      console.log(err, res1);
//
//      console.timeEnd("searcher");
//      process.exit(0);
//    });
//}
//
//testSearch();

//code, collection, text, condition, callback

//searcher.fullTextSearch("LawyerOnline", "test"
//  , [["インデックス一括", 10], ["李林", 1]]
//  , {}
//  , function(err, res) {
//    console.log(res);
//    console.timeEnd("searcher");
//    process.exit(0);
//  }
//);

searcher.similarSearch("LawyerOnline", "test"
  , [["インデックス一括", 10], ["李林", 1]], {}
  , function(err, res) {
    console.log(res);
    console.timeEnd("searcher");
    process.exit(0);
  }
);
