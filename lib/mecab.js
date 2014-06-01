var exec = require('child_process').exec
  ;

// for backward compatibility
var MeCab = function () {
};

MeCab.prototype = {
  _format: function (arrayResult) {
    var result = [];
    // Reference: http://mecab.googlecode.com/svn/trunk/mecab/doc/index.html
    // 表層形\t品詞,品詞細分類1,品詞細分類2,品詞細分類3,活用形,活用型,原形,読み,発音
    arrayResult.forEach(function (parsed) {
      if (parsed.length <= 8) {
        return;
      }
      result.push({
        kanji: parsed[0],
        lexical: parsed[1],
        compound: parsed[2],
        compound2: parsed[3],
        compound3: parsed[4],
        conjugation: parsed[5],
        inflection: parsed[6],
        original: parsed[7],
        reading: parsed[8],
        pronunciation: parsed[9] || ''
      });
    });
    return result;
  },
  _parseMeCabResult: function (result) {
    var ret = [];
    result.split('\n').forEach(function (line) {
      ret.push(line.replace('\t', ',').split(','));
    });
    return ret;
  },
  parse: function (str, option, callback) {
    var opt = MeCab.makeOption(option);
    str = str.replace(/\"/g, "\\\"");
    exec('echo "' + str + '" | mecab' + opt, function (err, result) {
      if (err || !str) {
        return callback(err, []);
      }
      return callback(null, MeCab._parseMeCabResult(result).slice(0, -2));
    });
  },
  parseFormat: function (str, option, callback) {
    MeCab.parse(str, option, function (err, result) {
      return callback(err, MeCab._format(result));
    });
  },
  _wakatsu: function (arr) {
    var ret = [];
    for (var i in arr) {
      ret.push(arr[i][0]);
    }
    return ret;
  },
  wakachi: function (str, option, callback) {
    MeCab.parse(str, option, function (err, arr) {
      if (err) {
        return callback(err, null);
      }
      return callback(null, MeCab._wakatsu(arr));
    });
  },
  makeDictionary: function (file, option, callback) {
    var opt = MeCab.makeOption(option);
    exec('mecab-dict-index' + opt + " " + file, function (err) {
      return callback(null);
    });
  },
  makeOption: function (option) {
    var result = "";
    for (var key in option) {
      result += ' --' + key + '="' + option[key] + '"';
    }
    return result;
  }
};

for (var x in MeCab.prototype) {
  MeCab[x] = MeCab.prototype[x];
}

module.exports = MeCab;
