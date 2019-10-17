package cn.lgwen;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.util.LinkedList;
import java.util.List;

/**
 * 2019/10/12
 * aven.wu
 * danxieai258@163.com
 */
public class JSQLPaserExample {

    private static String PRIMARY_KEY = "rowKey";

    public static void main(String[] args) throws Exception{
        String sql1 = "SELECT a, b as xin , c FROM `dwd_event*` WHERE value >= 1 and TABLE1.age < b.age AND 1 like '123' order by a, `c`.a desc limit 10";

        //String sql1 = "SELECT a, b, c FROM TABLE1 WHERE c.value like '%1'";

        Statement statement = CCJSqlParserUtil.parse(sql1);

        // 提取表名
        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
        List<String> tableList = tablesNamesFinder.getTableList(statement);
        System.out.println(tableList);

        Select select = (Select) statement;
        PlainSelect selectBody = (PlainSelect)select.getSelectBody();
        long offset = ((LongValue)selectBody.getLimit().getRowCount()).getValue();

        // 获取所有需要查询的列
        List<String> columnName = new LinkedList<>();

        List<SelectItem> selectItems = selectBody.getSelectItems();
        for (SelectItem item :selectItems) {
            if (item instanceof SelectExpressionItem) {
                columnName.add(((SelectExpressionItem)item).toString());
            }
        }

        List<OrderByElement> orderByElements = selectBody.getOrderByElements();

        Expression expression =  selectBody.getWhere();

        if (expression instanceof EqualsTo) {
         // 单个条件equal
            EqualsTo equalsTo = (EqualsTo)expression;
            String clumnName = ((Column)equalsTo.getLeftExpression()).getName(false);
            if(clumnName.equals(PRIMARY_KEY)) {
                // 匹配主键
                getBuilder(expression);
            }
        } else {

        }
    }

    private static void queryBuilder(List<InerExpression> list, Expression expression) {
        if(expression instanceof AndExpression) {
            Expression left = ((AndExpression) expression).getLeftExpression();
            if (left instanceof AndExpression) {
                queryBuilder(list, expression);
            } else {
                loadExpression(list, expression);
            }
        } else {
            loadExpression(list, expression);
        }
    }

    private static void loadExpression(List<InerExpression> list, Expression expression) {
        InerExpression exps = new InerExpression();
        if (expression instanceof EqualsTo) {
            exps.operate = "=";
            Column left = (Column)((EqualsTo) expression).getLeftExpression();
            exps.left = left.getColumnName();
            Expression rightExpression = ((EqualsTo) expression).getRightExpression();
            if (rightExpression instanceof LongValue) {
                LongValue longValue = ((LongValue) ((EqualsTo) expression).getRightExpression());
                exps.setRight(longValue.getStringValue());
            }
            if (rightExpression instanceof StringValue) {
                StringValue stringValue = ((StringValue) ((EqualsTo) expression).getRightExpression());
                exps.setRight(stringValue.getValue());
            }

        } else if (expression instanceof GreaterThan) {
            exps.operate = ">";
            Column left = (Column)((GreaterThan) expression).getLeftExpression();
            exps.left = left.getColumnName();
            Expression rightExpression = ((GreaterThan) expression).getRightExpression();
            if (rightExpression instanceof LongValue) {
                LongValue longValue = ((LongValue) ((GreaterThan) expression).getRightExpression());
                exps.setRight(longValue.getStringValue());
            }
            if (rightExpression instanceof StringValue) {
                StringValue stringValue = ((StringValue) ((GreaterThan) expression).getRightExpression());
                exps.setRight(stringValue.getValue());
            }

        } else if (expression instanceof MinorThan) {
            exps.operate = "<";
            Column left = (Column)((MinorThan) expression).getLeftExpression();
            exps.left = left.getColumnName();
            Expression rightExpression = ((MinorThan) expression).getRightExpression();
            if (rightExpression instanceof LongValue) {
                LongValue longValue = ((LongValue) ((MinorThan) expression).getRightExpression());
                exps.setRight(longValue.getStringValue());
            }
            if (rightExpression instanceof StringValue) {
                StringValue stringValue = ((StringValue) ((MinorThan) expression).getRightExpression());
                exps.setRight(stringValue.getValue());
            }
        }
        list.add(exps);
    }

    private static void getBuilder(Expression expression) {

    }


    public static class InerExpression {
        public static final String placeholderPrefix = "${";

        String left;
        Object right;
        String operate;
        // 是否占位符，需要替换
        boolean placeholder;

        private void setRight(String field) {
            String s = field.trim();
            if (s.startsWith(placeholderPrefix)) {
                placeholder = true;
            }
            right = s;
        }
    }
}
