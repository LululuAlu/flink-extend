package cn.lgwen.join;

import cn.lgwen.User;
import cn.lgwen.join.elasticsearch.AsyncElasticSearch6JoinFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 2019/10/17
 * aven.wu
 * danxieai258@163.com
 */
public class Elastic6JoinTest {

    @Test
    public void join() throws Exception{
        List<User> list = Collections.singletonList(new User("w6hX2G0BLkbAw9uARM5B"));
        HttpHost httpHost = new HttpHost("10.20.128.210", 19201, "http");
        String SQL = "SELECT name, age, salary from user where _id = ?";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<User> source = env.fromCollection(list);
        DataStream<User> join = AsyncDataStream.unorderedWait(source,
                new AsyncElasticSearch6JoinFunction<User>(SQL,
                        Arrays.asList("rowKey"),
                        new InjectValueByResultKeyJoinFunction<User, User, Object>(),
                        new HttpHost[]{httpHost},
                        "data"
                ), 50, TimeUnit.MINUTES, 5000);
        join.print();
        env.execute();
    }


    @Test
    public void joinOrder() throws Exception{
        List<User> list = Collections.singletonList(new User("w6hX2G0BLkbAw9uARM5B"));
        HttpHost httpHost = new HttpHost("10.20.128.210", 19201, "http");
        String SQL = "SELECT name, age, salary from user where _id = ?";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<User> source = env.fromCollection(list);
        DataStream<User> join = AsyncDataStream.unorderedWait(source,
                new AsyncElasticSearch6JoinFunction<User>(SQL,
                        Arrays.asList("rowKey"),
                        new InjectValueByResultKeyJoinFunction<User, User, Object>(),
                        new HttpHost[]{httpHost},
                        "data"
                ), 50, TimeUnit.MINUTES, 5000);
        join.print();
        env.execute();
    }
}
