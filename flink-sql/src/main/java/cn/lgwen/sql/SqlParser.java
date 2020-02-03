package cn.lgwen.sql;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.util.LinkedList;
import java.util.List;

/**
 * 2019/10/14
 * aven.wu
 * danxieai258@163.com
 */
public class SqlParser {


    public static SearchBuilder parserSqlToBuilder(String sql, SearchBuilder builder) throws Exception {

        Statement statement = CCJSqlParserUtil.parse(sql);
        // 提取表名
        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
        List<String> tableList = tablesNamesFinder.getTableList(statement);
        builder.setTableName(tableList.get(0));

        Select select = (Select) statement;
        PlainSelect selectBody = (PlainSelect)select.getSelectBody();

        //获取 limit
        if (selectBody.getLimit() != null) {
            builder.limit(((LongValue)selectBody.getLimit().getRowCount()).getValue());
        }

        // 获取所有需要查询的列
        List<SelectExpressionItem> columnName = new LinkedList<>();
        List<SelectItem> selectItems = selectBody.getSelectItems();
        for (SelectItem item :selectItems) {
            if (item instanceof SelectExpressionItem) {
                columnName.add((SelectExpressionItem)item);
            }
        }
        builder.setSelectItems(columnName);
        // 排序
        List<OrderByElement> orderByElements = selectBody.getOrderByElements();
        builder.orderBy(orderByElements);

        Expression expression =  selectBody.getWhere();
        if (expression instanceof EqualsTo) {
            // 单个条件equal
            EqualsTo equalsTo = (EqualsTo)expression;
            String column = ((Column)equalsTo.getLeftExpression()).getColumnName().replaceAll("`", "");
            if(column.equals(builder.primaryKey())) {
                // 匹配主键
                builder.builderGet(expression);
            } else {
                builder.builderQuery(expression);
            }
        } else {
            builder.builderQuery(expression);
        }

        return builder;
    }
}
