package cn.lgwen.sql;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import java.util.List;

/**
 * 2019/10/14
 * aven.wu
 * danxieai258@163.com
 * 根据SQL 构建自己的查询实现
 */
public interface SearchBuilder {

    String primaryKey();

    void limit(long limit);

    long getLimit();

    /**
     * 单或多条件，查询
     * @param expression
     */
    void builderQuery(Expression expression);

    /**
     * 主键获取，exp：id = 1
     * @param expression
     */
    void builderGet(Expression expression);

    /**
     * 设置查询的列
     * @param items
     */
    void setSelectItems(List<SelectExpressionItem> items);

    /**
     * 所查询的表名
     * @param tableName
     */
    void setTableName(String tableName);

    void orderBy(List<OrderByElement> orderByElements);

}
