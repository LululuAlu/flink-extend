## SQL相关

### SQLBuilder
解析SQL转成对应数据库的查询语句
* HBase  
``HBaseSearchBuilder``
```roomsql
select `office info`.post, `office info`.salary, `person info`.age, `person info`.name from user where `person info`.age >= ?
```
使用``"`office info`.post"`` 表示"family.qualifier"。
    1. 支持rowkey get
    2. 支持rowkey 前缀匹配
    3. 支持qualifier 精准匹配查询
    4. 只支持单表查询

* ElasticSearch