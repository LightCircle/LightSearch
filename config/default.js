/**
 * @file 应用程序配置文件
 * @author r2space@gmail.com
 */

module.exports = {

  /**
   * 数据库连接信息
   */
  "db": {
      "host": "127.0.0.1"           /* 数据库服务器地址 */
    , "port": 27017                 /* 数据库服务器端口 */
    , "dbname": "LawyerOnline"      /* 数据库名称 */
    , "pool": 5                     /* 连接池个数 */
    , "prefix": ""                  /* collection名的前缀 */
  }
};
