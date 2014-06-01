

## 使用maven
下载

    http://maven.apache.org/

指定PATH，JAVA_HOME即可使用

    export PATH=$PATH:/Users/lilin/developer/maven/bin

## 使用maven编译java代码
编译好的class文件会在java/target目录下

    $ mvn compiler:compile

## 关于例子
是使用tika，抽取文件中的文本的内容的
需要tika-app http://tika.apache.org/