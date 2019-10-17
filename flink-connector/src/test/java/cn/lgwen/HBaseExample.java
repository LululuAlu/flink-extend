package cn.lgwen;

import cn.lgwen.join.hbase.HBaseDDLExample;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.Test;

import java.util.List;
import java.util.Random;

/**
 * 2019/9/30
 * aven.wu
 * danxieai258@163.com
 */
public class HBaseExample {

    @Test
    public void insert() {
        String[] names = {"zhangsan","lisi","wangwu","tony","lilei","hanmeimei","xiaonaofu"};
        Random rm = new Random();
        for (int i = 0 ; i < 100;) {
            String name = names[rm.nextInt(names.length)];
            insert(name, String.valueOf(rm.nextInt(60) + 20));
            i++;
        }

    }

    @Test
    public void insertCare() {
        for (int i = 6; i >= 0; i --) {
            Put put = new Put("benz".getBytes());
            put.addColumn("info".getBytes(), "horsepower ".getBytes(), "300".getBytes());
            put.addColumn("info".getBytes(), "weight".getBytes(), "1542KG".getBytes());
            put.addColumn("info".getBytes(), "timestamp".getBytes(), String.valueOf(System.currentTimeMillis()).getBytes());
            Configuration configuration = HBaseDDLExample.getConf();
            try (Connection connection = ConnectionFactory.createConnection(configuration)){
                connection.getTable(TableName.valueOf("care")).put(put);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }


    @Test
    public void inserSinge() {
        insert("zhangsan", "11");
    }

    private static void insert(String name, String age) {
        Put put = new Put((name + "_"+ age +"_" + System.currentTimeMillis()).getBytes());
        put.addColumn("person info".getBytes(), "name".getBytes(), name.getBytes());
        put.addColumn("person info".getBytes(), "age".getBytes(), age.getBytes());
        put.addColumn("office info".getBytes(), "post".getBytes(), "coder".getBytes());
        put.addColumn("office info".getBytes(), "salary".getBytes(), "15K".getBytes());
        Configuration configuration = HBaseDDLExample.getConf();
        try (Connection connection = ConnectionFactory.createConnection(configuration)){
            connection.getTable(TableName.valueOf("user")).put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getByRowKey() {
        String rowKey = "zhangsan_11_1569822647799";
        String tableName = "user";
        try (Connection connection = HBaseDDLExample.connection()){
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                String column = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("column: " + column + "\nvalue:" + value);
            }

        } catch (Exception e) {

        }

    }

    @Test
    public void scan() {
        try (Connection connection = HBaseDDLExample.connection()){
            Table table = connection.getTable(TableName.valueOf("user"));
            Scan scan = new Scan();
            /*RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
                    new BinaryComparator("zhangsan_11_1569822647799".getBytes()));
            scan.setFilter(rowFilter);*/
            scan.setRowPrefixFilter("zhangsan".getBytes());
            ResultScanner rs = table.getScanner(scan);
            for (Result result : rs) {
                printResult(result);
            }
        }catch (Exception e) {

        }
    }

    @Test
    public void scanColumnFilter() {
        try (Connection connection = HBaseDDLExample.connection()){
            Table table = connection.getTable(TableName.valueOf("user"));
            Scan scan = new Scan();
            //scan.setRowPrefixFilter("zhangsan".getBytes());

            SingleColumnValueFilter sgf = new SingleColumnValueFilter("person info".getBytes(), "age".getBytes(),
                    CompareFilter.CompareOp.EQUAL, "30".getBytes());
            PageFilter pgfilter = new PageFilter(5);
            FilterList filterList = new FilterList(sgf, pgfilter);
            scan.setFilter(filterList);
            ResultScanner results = table.getScanner(scan);
            for (Result rs : results) {
                printResult(rs);
            }

        }catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void printResult(Result result) {
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            String row = Bytes.toString(CellUtil.cloneRow(cell));
            String cf = Bytes.toString(CellUtil.cloneFamily(cell));
            String cq = Bytes.toString(CellUtil.cloneQualifier(cell));
            String val = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println(row + "/" + cf + "/" + cq + "/" + val);
        }
    }
}
