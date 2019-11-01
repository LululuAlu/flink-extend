package cn.lgwen.jdbc;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

/**
 * 2019/10/25
 * aven.wu
 * danxieai258@163.com
 */
public class SimpleDateSource implements DataSource {

    private static final String dirverClassName = "com.mysql.jdbc.Driver";
    private static final String url = "jdbc:mysql://10.20.128.184:18888/t_test?useUnicode=true&characterEncoding=utf8&autoReconnect=true&rewriteBatchedStatements=TRUE";
    private static final String user = "root";
    private static final String pswd = "1qazCDE#5tgb";

    //连接池
    private static List<Connection> pool = Collections.synchronizedList(new LinkedList<Connection>());
    private static SimpleDateSource instance = new SimpleDateSource();

    static {
        try {
            Class.forName(dirverClassName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private SimpleDateSource() {

    }

    /**
     * 获取数据源单例
     *
     * @return 数据源单例
     */
    public static SimpleDateSource instance() {
        return instance;
    }

    /**
     * 获取一个数据库连接
     *
     * @return 一个数据库连接
     * @throws SQLException
     */
    public Connection getConnection() throws SQLException {
        synchronized (pool) {
            if (pool.size() > 0) return pool.remove(0);
            else return makeConnection();
        }
    }

    /**
     * 连接归池
     *
     * @param conn
     */
    public static void freeConnection(Connection conn) {
        pool.add(conn);
    }

    private Connection makeConnection() throws SQLException {
        return DriverManager.getConnection(url, user, pswd);
    }

    public Connection getConnection(String username, String password) throws SQLException {
        return DriverManager.getConnection(url, username, password);
    }

    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    public void setLogWriter(PrintWriter out) throws SQLException {

    }

    public void setLoginTimeout(int seconds) throws SQLException {

    }

    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
