package cn.lgwen.sql;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;


import java.util.List;

/**
 * 2019/10/15
 * aven.wu
 * danxieai258@163.com
 */
public abstract class AbstractSearchBuilder implements SearchBuilder {
    public static final String AND = "and";

    public static final String OR = "or";

    protected List<OrderByElement> orderByElements;

    protected void queryBuilder(List<InnerExpression> list, Expression expression, String condition) {
        if (expression instanceof AndExpression) {
            Expression left = ((AndExpression) expression).getLeftExpression();
            if (left instanceof AndExpression) {
                queryBuilder(list, left, AND);
            } else if (left instanceof OrExpression) {
                queryBuilder(list, left, OR);
            }else {
                loadExpression(list, left, condition);
            }
            loadExpression(list, ((AndExpression) expression).getRightExpression(), condition);
        } else {
            loadExpression(list, expression, condition);
        }
    }

    protected void loadExpression(List<InnerExpression> list, Expression expression, String condition) {
        InnerExpression exps = new InnerExpression();
        exps.condition = condition;
        if (expression instanceof EqualsTo) {
            exps.operate = "=";
            Column left = (Column) ((EqualsTo) expression).getLeftExpression();
            exps.left = selectItemLeftColumnName(left);
            Expression rightExpression = ((EqualsTo) expression).getRightExpression();
            if (rightExpression instanceof LongValue) {
                LongValue longValue = ((LongValue) ((EqualsTo) expression).getRightExpression());
                exps.setRight(longValue.getStringValue());
            } else
            if (rightExpression instanceof StringValue) {
                StringValue stringValue = ((StringValue) ((EqualsTo) expression).getRightExpression());
                exps.setRight(stringValue.getValue());
            } else if (rightExpression instanceof JdbcParameter) {
                // JdbcParameter stringValue = ((JdbcParameter) ((LikeExpression) expression).getRightExpression());
                // TODO jdbcParam 可以获取参数的index
                exps.setRight("?");
            }

        } else if (expression instanceof GreaterThan) {
            exps.operate = ">";
            Column left = (Column) ((GreaterThan) expression).getLeftExpression();
            exps.left = selectItemLeftColumnName(left);
            Expression rightExpression = ((GreaterThan) expression).getRightExpression();
            if (rightExpression instanceof LongValue) {
                LongValue longValue = ((LongValue) ((GreaterThan) expression).getRightExpression());
                exps.setRight(longValue.getStringValue());
            } else
            if (rightExpression instanceof StringValue) {
                StringValue stringValue = ((StringValue) ((GreaterThan) expression).getRightExpression());
                exps.setRight(stringValue.getValue());
            } else if (rightExpression instanceof JdbcParameter) {
                // JdbcParameter stringValue = ((JdbcParameter) ((LikeExpression) expression).getRightExpression());
                // TODO jdbcParam 可以获取参数的index
                exps.setRight("?");
            }

        } else if (expression instanceof GreaterThanEquals) {
            exps.operate = ">=";
            Column left = (Column) ((GreaterThanEquals) expression).getLeftExpression();
            exps.left = selectItemLeftColumnName(left);
            Expression rightExpression = ((GreaterThanEquals) expression).getRightExpression();
            if (rightExpression instanceof LongValue) {
                LongValue longValue = ((LongValue) ((GreaterThanEquals) expression).getRightExpression());
                exps.setRight(longValue.getStringValue());
            } else
            if (rightExpression instanceof StringValue) {
                StringValue stringValue = ((StringValue) ((GreaterThanEquals) expression).getRightExpression());
                exps.setRight(stringValue.getValue());
            } else if (rightExpression instanceof JdbcParameter) {
                // JdbcParameter stringValue = ((JdbcParameter) ((LikeExpression) expression).getRightExpression());
                // TODO jdbcParam 可以获取参数的index
                exps.setRight("?");
            }

        } else if (expression instanceof MinorThan) {
            exps.operate = "<";
            Column left = (Column) ((MinorThan) expression).getLeftExpression();
            exps.left = selectItemLeftColumnName(left);
            Expression rightExpression = ((MinorThan) expression).getRightExpression();
            if (rightExpression instanceof LongValue) {
                LongValue longValue = ((LongValue) ((MinorThan) expression).getRightExpression());
                exps.setRight(longValue.getStringValue());
            } else
            if (rightExpression instanceof StringValue) {
                StringValue stringValue = ((StringValue) ((MinorThan) expression).getRightExpression());
                exps.setRight(stringValue.getValue());
            } else if (rightExpression instanceof JdbcParameter) {
                // JdbcParameter stringValue = ((JdbcParameter) ((LikeExpression) expression).getRightExpression());
                // TODO jdbcParam 可以获取参数的index
                exps.setRight("?");
            }
        }else if (expression instanceof MinorThanEquals) {
            exps.operate = "<=";
            Column left = (Column) ((MinorThanEquals) expression).getLeftExpression();
            exps.left = selectItemLeftColumnName(left);
            Expression rightExpression = ((MinorThanEquals) expression).getRightExpression();
            if (rightExpression instanceof LongValue) {
                LongValue longValue = ((LongValue) ((MinorThanEquals) expression).getRightExpression());
                exps.setRight(longValue.getStringValue());
            } else if (rightExpression instanceof StringValue) {
                StringValue stringValue = ((StringValue) ((MinorThanEquals) expression).getRightExpression());
                exps.setRight(stringValue.getValue());
            } else if (rightExpression instanceof JdbcParameter) {
                // JdbcParameter stringValue = ((JdbcParameter) ((LikeExpression) expression).getRightExpression());
                // TODO jdbcParam 可以获取参数的index
                exps.setRight("?");
            }
        } else if (expression instanceof LikeExpression) {
            exps.operate = "like";
            Column left = (Column) ((LikeExpression) expression).getLeftExpression();
            exps.left = selectItemLeftColumnName(left);
            Expression rightExpression = ((LikeExpression) expression).getRightExpression();
            if (rightExpression instanceof LongValue) {
                LongValue longValue = ((LongValue) ((LikeExpression) expression).getRightExpression());
                exps.setRight(longValue.getStringValue());
            }
            if (rightExpression instanceof StringValue) {
                StringValue stringValue = ((StringValue) ((LikeExpression) expression).getRightExpression());
                exps.setRight(stringValue.getValue().replaceAll("%", ""));
            }
            if (rightExpression instanceof JdbcParameter) {
                // JdbcParameter stringValue = ((JdbcParameter) ((LikeExpression) expression).getRightExpression());
                // TODO jdbcParam 可以获取参数的index
                exps.setRight("?");
            }
        }
        list.add(exps);
    }

    /**
     * 不同的数据库 实现不同的 setColumn逻辑
     * @param column
     * @return
     */
    protected abstract String selectItemLeftColumnName(Column column);

    @Override
    public void orderBy(List<OrderByElement> orderByElements) {
        this.orderByElements = orderByElements;
    }

    public class InnerExpression {

        public static final String placeholder = "?";

        String left;
        Object right;
        String operate;
        // 存在组合条件的时候出现，支持 AND OR
        String condition;

        String alias;

        public void setRight(String field) {
            right = field.trim();
        }

        // 是否占位符，true 表示需要替换
        boolean isPlaceholder() {
            return placeholder.equals(right);
        }
    }

}
