package cn.lgwen.jdbc;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 2019/10/25
 * aven.wu
 * danxieai258@163.com
 */
public class JDBCTest {

    private static final String dirverClassName = "com.mysql.jdbc.Driver";
    private static final String url = "jdbc:mysql://10.20.128.184:18888/t_test?useUnicode=true&characterEncoding=utf8&autoReconnect=true&rewriteBatchedStatements=TRUE";
    private static final String user = "root";
    private static final String pswd = "1qazCDE#5tgb";

    Connection connection;


    @Before
    public void loadDataFixtures() throws Exception {
        Class.forName(dirverClassName);
        connection =  DriverManager.getConnection(url, user, pswd);
        //ScriptRunner runner = new ScriptRunner(connection);
        //runner.setEscapeProcessing(false);
        //runner.setSendFullScript(true);
        //runner.runScript(new InputStreamReader(JDBCTest.class.getClassLoader().getResourceAsStream("data.sql"), "UTF-8"));
    }

    @Test
    public void test1() throws Exception{
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM `user` WHERE id = 1");
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            String valeue = rs.getString("user_name");
            Assert.assertEquals(valeue, "小王");
        }
    }

}
