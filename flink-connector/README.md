## Connector

### JOIN 

#### elasticSearch join
基于SQL Builder 模块的SQL解析和ES语句查询的关联
``AsyncElasticSearch6JoinFunction<IN>``。定义SQL ``SELECT user_name as ,age from user where id = ?``,从指定index(支持多索引,服务es规则即可)
查询数据，``where`` 条件的"值"由被join数据提供,根据构造参数``matchField``从被join数据中获取对应值，按顺序填充SQL占位符
Example:
```java
List<User> list = Collections.singletonList(new User("w6hX2G0BLkbAw9uARM5B"));
HttpHost httpHost = new HttpHost("es.host", 9200, "http");
String SQL = "SELECT name, age, salary from user where id = ?";
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStreamSource<User> source = env.fromCollection(list);
DataStream<User> join = AsyncDataStream.unorderedWait(source,
          new AsyncElasticSearch6JoinFunction<User>(SQL,
              Arrays.asList("id"),
              new InjectValueByResultKeyJoinFunction<User, User, Object>(),
              new HttpHost[]{httpHost},
                   "data"
          ), 50, TimeUnit.MINUTES, 5000);
join.print();
env.execute();
```

#### HBase join
``AsyncHBaseJoinFunction14<IN>``SQL定义。
```roomsql
select `person info`.age as age, `person info`.name as name from user where rowkey = ?
```
"`person info`.age" 表示family=person info, qualifier=age,指定rowKey的关键字为rowkey

Example
```java
List<User> list = Collections.singletonList(new User( "1"));
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStreamSource<User> source = env.fromCollection(list);
DataStream<User> join = AsyncDataStream.unorderedWait(source,
               new AsyncHBaseJoinFunction14<User>(
                       "select `person info`.age as age, `person info`.name as name from user where rowkey = ?",
                       Collections.singletonList("rowKey"),
                new InjectValueByResultKeyJoinFunction<User, User, String>()
      ), 50, TimeUnit.MINUTES, 5000);
join.print();
env.execute();
```

#### JDBC join
`AsyncJDBCJoinFunction<IN>`  
构造函数,指定一个返回dataSource的function，必须实现Serializable
```java
/**
 *
 * @param SQL
 * @param matchField POJD param field
 * @param joinFunction 
 * @param dataSourceSupplier dataSourceSupplier must implement Serializable
 */
public AsyncJDBCJoinFunction(String SQL,
                             List<String> matchField,
                             BiFunction<IN, List<Map<String, Object>>, IN> joinFunction,
                             Supplier<DataSource> dataSourceSupplier) {
    super(SQL, matchField, joinFunction);
    this.dataSourceSupplier = dataSourceSupplier;
}
```
Example
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStreamSource<User> source = env.fromCollection(Collections.singleton(new User(1)));
String SQL = "SELECT user_name as userName, age, point_value as pointValue from user where id = ?";
DataStream<User> join = AsyncDataStream.unorderedWait(source,
            new AsyncJDBCJoinFunction<User>(SQL,
                    Arrays.asList("id"),
                    new InjectValueByResultKeyJoinFunction<User, User, Object>(),
                    new Supplier<DataSource>(){}
            ), 50, TimeUnit.MINUTES, 5000);
join.print();
env.execute();
```