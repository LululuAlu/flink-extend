package cn.lgwen.join;

import cn.lgwen.User;
import cn.lgwen.join.hbase.AsyncHBaseJoinFunction14;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 2019/10/16
 * aven.wu
 * danxieai258@163.com
 */
public class HBaseJoinTest {

    @Test
    public void test() throws Exception {
        List<User> list = Collections.singletonList(new User( "xiaonaofu_38_1569825379206"));

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
    }
}
