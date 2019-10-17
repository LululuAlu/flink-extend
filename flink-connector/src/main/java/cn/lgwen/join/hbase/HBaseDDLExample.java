package cn.lgwen.join.hbase;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;

import java.io.IOException;

/**
 * 2019/9/27
 * aven.wu
 * danxieai258@163.com
 */
@Slf4j
public class HBaseDDLExample {

    public static void main(String[] args) {
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf("care"));
        table.addFamily(new HColumnDescriptor("info").setCompressionType(Compression.Algorithm.NONE).setMaxVersions(5));
        createSchemaTable(getConf(), table);

    }

    public static void createSchemaTable(Configuration configuration, HTableDescriptor table) {
        try (Connection connection = connection();
             Admin admin = connection.getAdmin()){
            if (admin.tableExists(table.getTableName())) {
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }
            admin.createTable(table);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void modifySchemaTable(Configuration configuration, String tableN) {
        try (Connection connection = connection();
             Admin admin = connection.getAdmin()){
            TableName tableName = TableName.valueOf(tableN);
            if(!admin.tableExists(tableName)) {
                return;
            }
            HTableDescriptor table = admin.getTableDescriptor(tableName);

            // Update existing table
            HColumnDescriptor newColumn = new HColumnDescriptor("NEWCF");
            newColumn.setCompactionCompressionType(Compression.Algorithm.GZ);
            newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            admin.addColumn(tableName, newColumn);

            // Update existing column family
            HColumnDescriptor existingColumn = new HColumnDescriptor("");
            existingColumn.setCompactionCompressionType(Compression.Algorithm.GZ);
            existingColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            table.modifyFamily(existingColumn);
            admin.modifyTable(tableName, table);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void dropSchemaTable(Configuration configuration, HTableDescriptor table) {
        try (Connection connection = connection();
             Admin admin = connection.getAdmin()){
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Connection connection() throws IOException {
        return ConnectionFactory.createConnection(getConf());
    }


    public static Configuration getConf() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(HBaseDDLExample.class.getClassLoader().getResource("core-site.xml"));
        configuration.addResource(HBaseDDLExample.class.getClassLoader().getResource("hbase-site.xml"));
        configuration.set("hbase.client.ipc.pool.type", "");
        configuration.set("hbase.client.ipc.pool.size", "5");
        return configuration;
    }
}
