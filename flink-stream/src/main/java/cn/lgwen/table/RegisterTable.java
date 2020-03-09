package cn.lgwen.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


import java.util.concurrent.TimeUnit;

public class RegisterTable {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);

        DataStreamSource<User> dataStream = fsEnv.addSource(new SourceFunction<User>() {

            @Override
            public void run(SourceContext<User> sourceContext) throws Exception {
                for (int i = 0; i < 1000000; i++) {
                    TimeUnit.MILLISECONDS.sleep(1000);
                    sourceContext.collect(new User(i, "Jack" + i, i % 2 == 0 ? "boy" : "girl"));
                    sourceContext.collect(new User(i, "Jack" + i, null));
                }
            }

            @Override
            public void cancel() {

            }
        });
        // 将流转换成一张表 注册
        fsTableEnv.registerDataStream("table1", dataStream, "id, name, birth, gender, UserActionTime.proctime");
        Table result = fsTableEnv.sqlQuery("select COUNT(*) pvcount, name, gender from table1");
        fsTableEnv.registerDataStream("user", dataStream);

//        Table count = fsTableEnv.sqlQuery("SELECT COUNT(*) as pvcount, TUMBLE_END(UserActionTime, INTERVAL '5' SECOND) as processTime, gender " +
//                "FROM table1 GROUP BY TUMBLE(UserActionTime, INTERVAL '5' SECOND), gender");
        DataStream<Row> userDataStream = fsTableEnv.toAppendStream(result,
                new RowTypeInfo(
                new RowTypeInfo( new TypeInformation[] {
                        TypeInformation.of(Long.class),
                        TypeInformation.of(String.class),
                        TypeInformation.of(Integer.class)
                },
                        new String[]{"pvcount", "name", "gender"}
        )));

        userDataStream.print();

        fsTableEnv.execute("my job");
    }
}
