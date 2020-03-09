package cn.lgwen.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 2020/3/6
 * aven.wu
 * danxieai258@163.com
 */
public class FlinkTableTest {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(env, fsSettings);
        DataStream<Row> soource = env.fromElements(
                Row.of(1, 10, "a"),
                Row.of(2, 2, "b"),
                Row.of(3, 12, "c"),
                Row.of(13, 14, "d")
        ).returns(new RowTypeInfo(
                new TypeInformation[]{TypeInformation.of(Integer.class),
                        TypeInformation.of(Integer.class),
                        TypeInformation.of(String.class)},
                new String[]{"id", "age", "name"}
        ));
        fsTableEnv.registerDataStream("table1", soource, "id, name, age");
        Table result = fsTableEnv.sqlQuery("select COUNT(*) pvcount, name from table1 GROUP BY age");

//        DataStream<Row> userDataStream = tenv.toAppendStream(result,
//                new RowTypeInfo(
//                        new RowTypeInfo( new TypeInformation[] {
//                                TypeInformation.of(Long.class),
//                                TypeInformation.of(String.class),
//                                TypeInformation.of(Integer.class)
//                        },
//                                new String[]{"pvcount", "name", "gender"}
//                        )));
//
//        userDataStream.print();

        fsTableEnv.execute("my job");
    }
}
