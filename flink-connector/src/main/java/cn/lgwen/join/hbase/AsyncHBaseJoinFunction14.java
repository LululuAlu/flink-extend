package cn.lgwen.join.hbase;

import cn.lgwen.join.AbstractAsyncJoinFunction;
import cn.lgwen.sql.HBase114SearchBuilder;
import cn.lgwen.sql.SqlParser;
import cn.lgwen.util.ReflectUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.hadoop.hbase.client.Connection;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * 2019/10/12
 * aven.wu
 * danxieai258@163.com
 * object must Implement getter setter
 */
public class AsyncHBaseJoinFunction14<O> extends AbstractAsyncJoinFunction<O, O, String> {

    private transient Connection connection;

    public AsyncHBaseJoinFunction14(String SQL,
                                    List<String> matchField,
                                    BiFunction<O, List<Map<String, String>>, O> joinFunction) {
        super(SQL, matchField, joinFunction);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = HBaseDDLExample.connection();
    }

    @Override
    public void asyncInvoke(O input, ResultFuture<O> resultFuture) throws Exception {
        HBase114SearchBuilder searchBuilder = new HBase114SearchBuilder();
        SqlParser.parserSqlToBuilder(this.SQL, searchBuilder);
        List<Object> attribute = ReflectUtil.reflectObjectAttribute(matchField, input);
        if (attribute.size() != matchField.size()) {
            resultFuture.complete(Collections.singleton(input));
        } else {
            List<Map<String, String>> query = searchBuilder.query(connection, attribute.toArray(new Object[]{}));
            O out = joinFunction.apply(input, query);
            resultFuture.complete(Collections.singleton(out));
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }
}
