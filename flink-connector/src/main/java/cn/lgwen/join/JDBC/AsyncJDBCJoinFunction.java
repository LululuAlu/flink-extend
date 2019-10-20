package cn.lgwen.join.JDBC;

import cn.lgwen.join.AbstractAsyncJoinFunction;
import cn.lgwen.util.ReflectUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.function.BiFunction;

/**
 * 2019/10/18
 * aven.wu
 * danxieai258@163.com
 */
public class AsyncJDBCJoinFunction<IN> extends AbstractAsyncJoinFunction<IN, IN, Object> {

    private transient DataSource dataSource;


    public AsyncJDBCJoinFunction(String SQL,
                                 List<String> matchField,
                                 BiFunction<IN, List<Map<String, Object>>, IN> joinFunction) {
        super(SQL, matchField, joinFunction);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // TODO init dataSource
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void asyncInvoke(IN input, ResultFuture<IN> resultFuture) throws Exception {
        Connection connection = dataSource.getConnection();
        List<Object> attribute = ReflectUtil.reflectObjectAttribute(matchField, input);
        PreparedStatement statement = connection.prepareStatement(SQL);
        if (!attribute.isEmpty()) {
            int i = 1;
            for(Object param : attribute) {
                statement.setObject(i, param);
                i++;
            }
        }
        ResultSet rs = statement.executeQuery();
        List<Map<String, Object>> resultList = new LinkedList<>();
        while (rs.next()) {
            ResultSetMetaData rsmd = rs.getMetaData();
            int count = rsmd.getColumnCount();
            Map<String, Object> resultMap = new HashMap<>();
            for(int index = 1; index <= count; index++){
                rs.getObject(index);
                resultMap.put(rsmd.getColumnName(index), rs.getObject(index));
            }
            resultList.add(resultMap);
        }
        connection.close();
        if(resultList.isEmpty()) {
            resultFuture.complete(Collections.singleton(input));
        }
        resultFuture.complete(Collections.singleton(joinFunction.apply(input, resultList)));
    }
}
