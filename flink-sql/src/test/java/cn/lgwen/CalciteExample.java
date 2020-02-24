package cn.lgwen;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Test;

/**
 * 2019/10/12
 * aven.wu
 * danxieai258@163.com
 */
public class CalciteExample {

    @Test
    public void example() throws Exception{

        String sql = "select * from emps where id = 1";
        // 解析配置
        SqlParser.Config mysqlConfig = SqlParser.configBuilder().setLex(Lex.MYSQL).build();
        // 创建解析器
        SqlParser parser = SqlParser.create(sql, mysqlConfig);
        // 解析sql
        SqlNode sqlNode = parser.parseQuery();
        // 还原某个方言的SQL
        System.out.println(sqlNode.toSqlString(OracleSqlDialect.DEFAULT));

    }
}
