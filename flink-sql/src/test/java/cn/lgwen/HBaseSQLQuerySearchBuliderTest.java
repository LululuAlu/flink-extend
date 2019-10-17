package cn.lgwen;

import cn.lgwen.sql.HBase114SearchBuilder;
import cn.lgwen.sql.SqlParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 2019/10/15
 * aven.wu
 * danxieai258@163.com
 */
public class HBaseSQLQuerySearchBuliderTest {

    Connection connection;

    public static Connection connection() throws IOException {
        return ConnectionFactory.createConnection(getConf());
    }

    public static Configuration getConf() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(HBaseSQLQuerySearchBuliderTest.class.getClassLoader().getResource("core-site.xml"));
        configuration.addResource(HBaseSQLQuerySearchBuliderTest.class.getClassLoader().getResource("hbase-site.xml"));
        configuration.set("hbase.client.ipc.pool.size", "5");
        return configuration;
    }

    @Before
    public void init() throws Exception{
        connection = connection();
    }

    @After
    public void destroy() throws Exception{
        connection.close();
    }


    @Test
    public void searchByRowKeyPrefix() throws Exception{
        String sql = "select `office info`.post, `office info`.salary, `person info`.age, `person info`.name from user where rowkey like ?";
        HBase114SearchBuilder builder = new HBase114SearchBuilder();
        SqlParser.parserSqlToBuilder(sql, builder);

        List<Map<String, String>> result = builder.query(connection, "zhangsan");
        System.out.println(result);
        Assert.assertTrue(result.get(0).get("rowkey").contains("zhangsan"));
    }

    @Test
    public void searchRowKeyGet() throws Exception{
        String sql = "select `office info`.post, `office info`.salary, `person info`.age, `person info`.name from user where rowkey = ?";
        HBase114SearchBuilder builder = new HBase114SearchBuilder();
        SqlParser.parserSqlToBuilder(sql, builder);

        List<Map<String, String>> result = builder.query(connection, "xiaonaofu_38_1569825379206");
        System.out.println(result);
        Assert.assertEquals(result.get(0).get("rowkey"), "xiaonaofu_38_1569825379206");
    }

    @Test
    public void searchRowForNoMatch() throws Exception{
        String sql = "select `office info`.post, `office info`.salary as salary , `person info`.age as age, `person info`.name as name from user where rowkey = ?";
        HBase114SearchBuilder builder = new HBase114SearchBuilder();
        SqlParser.parserSqlToBuilder(sql, builder);

        List<Map<String, String>> result = builder.query(connection, "xiaonaofu_38_156982537");
        System.out.println(result);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void searchRowKeyGetNoArg() throws Exception{
        String sql = "select `office info`.post, `office info`.salary, `person info`.age, `person info`.name from user where rowkey = 'xiaonaofu_38_1569825379206'";
        HBase114SearchBuilder builder = new HBase114SearchBuilder();
        SqlParser.parserSqlToBuilder(sql, builder);

        List<Map<String, String>> result = builder.query(connection, null);
        System.out.println(result);
        Assert.assertEquals(result.get(0).get("rowkey"), "xiaonaofu_38_1569825379206");
    }

    @Test
    public void searchColumnFilterNoArg() throws Exception{
        String sql = "select `office info`.post, `office info`.salary, `person info`.age, `person info`.name from user where `person info`.age = 30";
        HBase114SearchBuilder builder = new HBase114SearchBuilder();
        SqlParser.parserSqlToBuilder(sql, builder);

        List<Map<String, String>> result = builder.query(connection, null);
        System.out.println(result);
        Assert.assertEquals(result.get(0).get("`person info`.age"), "30");
    }

    @Test
    public void searchColumnFilterGreatThan() throws Exception{
        String sql = "select `office info`.post, `office info`.salary, `person info`.age, `person info`.name from user where `person info`.age > 30";
        HBase114SearchBuilder builder = new HBase114SearchBuilder();
        SqlParser.parserSqlToBuilder(sql, builder);

        List<Map<String, String>> result = builder.query(connection, null);
        System.out.println(result);
        Assert.assertTrue(Integer.valueOf(result.get(0).get("`person info`.age")) > 30);
    }

    @Test
    public void searchColumnFilterGreatThanEq() throws Exception{
        String sql = "select `office info`.post, `office info`.salary, `person info`.age, `person info`.name from user where `person info`.age >= 30";
        HBase114SearchBuilder builder = new HBase114SearchBuilder();
        SqlParser.parserSqlToBuilder(sql, builder);

        List<Map<String, String>> result = builder.query(connection, null);
        System.out.println(result);
        Assert.assertTrue(Integer.valueOf(result.get(0).get("`person info`.age")) > 30);
    }

    @Test
    public void searchColumnFilterLessThan() throws Exception{
        String sql = "select `office info`.post, `office info`.salary, `person info`.age, `person info`.name from user where `person info`.age < 30";
        HBase114SearchBuilder builder = new HBase114SearchBuilder();
        SqlParser.parserSqlToBuilder(sql, builder);

        List<Map<String, String>> result = builder.query(connection, null);
        System.out.println(result);
        Assert.assertTrue(Integer.valueOf(result.get(0).get("`person info`.age")) < 30);
    }

    @Test
    public void searchColumnFilter() throws Exception{
        String sql = "select `office info`.post, `office info`.salary, `person info`.age, `person info`.name from user where `person info`.age >= ?";
        HBase114SearchBuilder builder = new HBase114SearchBuilder();
        SqlParser.parserSqlToBuilder(sql, builder);

        List<Map<String, String>> result = builder.query(connection, "30");
        System.out.println(result);
        Assert.assertTrue(Integer.valueOf(result.get(0).get("`person info`.age")) >= 30);
    }

    @Test
    public void searchAndColumnFilter() throws Exception{
        String sql = "select `office info`.post, `office info`.salary, `person info`.age, `person info`.name from user where `person info`.age >= ? and `person info`.name = 'tony'";
        HBase114SearchBuilder builder = new HBase114SearchBuilder();
        SqlParser.parserSqlToBuilder(sql, builder);

        List<Map<String, String>> result = builder.query(connection, "30");
        System.out.println(result);
        Assert.assertTrue(Integer.valueOf(result.get(0).get("`person info`.age")) >= 30);    }


    @Test
    public void searchPage() throws Exception{
        String sql = "select `office info`.post, `office info`.salary, `person info`.age, `person info`.name from user where `person info`.age >= ? limit 5";
        HBase114SearchBuilder builder = new HBase114SearchBuilder();
        SqlParser.parserSqlToBuilder(sql, builder);

        List<Map<String, String>> result = builder.query(connection, "30");
        System.out.println(result);
        Assert.assertEquals(result.size(), 5);

    }

    @Test
    public void searchResultUseAlias() throws Exception{
        String sql = "select `office info`.post, `office info`.salary as salary , `person info`.age as age, `person info`.name as name from user where rowkey = ?";
        HBase114SearchBuilder builder = new HBase114SearchBuilder();
        SqlParser.parserSqlToBuilder(sql, builder);

        List<Map<String, String>> result = builder.query(connection, "xiaonaofu_38_1569825379206");
        System.out.println(result);
        Assert.assertEquals(result.get(0).get("rowkey"), "xiaonaofu_38_1569825379206");
    }

}
