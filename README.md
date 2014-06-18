LightSearch
===========

https://github.com/hecomi/node-mecab-async

mecab例子
http://www.edrdg.org/~jwb/mecabdemo.html

1. NBest
 东京大学 NBest=1 时 词语被分成1个
  NBest=2 时 词语被分成3个 东京，大学，东京大学

2. 相似度算法
 2.1 cosø

 2.2

 2.3

 2.4



Mecab环境
========
1. 词典路径
    /usr/local/Cellar/mecab/0.996/lib/mecab/dic

2.


计算方法
========
tf 单词频率 词出现的次数/句子整个词数
  反复出现的词更能代表改文章

idf 逆文档频率 log（所有文档个数/包含该单词的文档个数）
  在某个文档当中词只出现一次的话，该单词代表文档的意思
  使用idf在不同的文档中出现次数特别多的词可以被忽略。

tf * idf
  考虑上两个要素，值越大越重要

参考
=======
除了cos相似度以外

=> Jaccard相似度也比较常用
http://blog.csdn.net/xceman1997/article/details/8600277

=> simhash算法原理和代码实现
http://blog.sina.cn/dpool/blog/s/blog_81e6c30b0101cpvu.html


数据结构
========
k 汉字
r 读音
v {
  w 权重
  n 单词在给定句子中出现的次数

  tf tf值
  idf idf值
  tfidf tf乘idf的值

  count 单词在多少个文档中出现过

  weight 权重
  sum 给定句子的单词数
  total 总文档数
}


libmmseg 安装
=================

 首页: http://www.coreseek.cn/opensource/mmseg/

`yum install make gcc gcc-c++ libtool autoconf automake`

`wget http://www.coreseek.cn/uploads/csft/3.2/mmseg-3.2.14.tar.gz`

`tar zxvf mmseg-3.2.14.tar.gz`

`cd mmseg-3.2.14`

`./bootstrap`

`./configure --prefix=/usr/local/mmseg3`

`make && make install`

`ln -s /usr/local/mmseg3/bin/mmseg /bin/mmseg3`



