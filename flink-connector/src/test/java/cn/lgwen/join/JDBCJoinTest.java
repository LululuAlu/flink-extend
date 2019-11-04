package cn.lgwen.join;

import cn.lgwen.jdbc.SimpleDateSource;
import cn.lgwen.join.JDBC.AsyncJDBCJoinFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Assert;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 2019/11/1
 * aven.wu
 * danxieai258@163.com
 */
public class JDBCJoinTest {


    @Test
    public void joinById() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<User> source = env.fromCollection(Collections.singleton(new User(1)));
        String SQL = "SELECT user_name as userName, age, point_value as pointValue from user where id = ?";
        DataStream<User> join = AsyncDataStream.unorderedWait(source,
                new AsyncJDBCJoinFunction<User>(SQL,
                        Arrays.asList("id"),
                        new InjectValueByResultKeyJoinFunction<User, User, Object>(),
                        new A()
                ), 50, TimeUnit.MINUTES, 5000);
        join.print();
        join.filter(x -> {
            Assert.assertEquals(x.getUserName(), "小王");
            Assert.assertEquals(x.getAge(), 25);
            Assert.assertEquals(x.getPointValue(), "3");
            return true;
        });
        env.execute();

    }

    public static class A implements Supplier<DataSource>, Serializable {

        @Override
        public DataSource get() {
            return SimpleDateSource.instance();
        }
    }



    public static class User implements Serializable {
        public User() {
        }

        public User(int id) {
            this.id = id;
        }

        private int id;

        private String userName;

        private int age;

        private String pointValue;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public String getPointValue() {
            return pointValue;
        }

        public void setPointValue(String pointValue) {
            this.pointValue = pointValue;
        }

        @Override
        public String toString() {
            return "User{" +
                    "id=" + id +
                    ", userName='" + userName + '\'' +
                    ", age=" + age +
                    ", pointValue='" + pointValue + '\'' +
                    '}';
        }
    }
}
