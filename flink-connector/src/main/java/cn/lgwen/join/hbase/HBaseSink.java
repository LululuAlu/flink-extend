package cn.lgwen.join.hbase;

import com.sun.istack.internal.NotNull;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;


/**
 * 2019/9/30
 * aven.wu
 * danxieai258@163.com
 */
@NoArgsConstructor
public class HBaseSink<T extends Row> extends RichSinkFunction<T> {

    private transient Connection connection;

    private String tableName;

    private String familyName;

    public HBaseSink(
            @NotNull String tableName, @NotNull String familyName) {
        this.tableName = tableName;
        this.familyName = familyName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = HBaseDDLExample.connection();
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        try (Table table = connection.getTable(TableName.valueOf(tableName))){
            Put put = new Put(value.getRow());
            //TODO 反射获取所有的字段
            put.addColumn(familyName.getBytes(), "".getBytes(), "".getBytes());
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(connection != null) {
            connection.close();
        }
    }
}
