package cn.lgwen.sql;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.util.Asserts;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;

/**
 * 2019/10/14
 * aven.wu
 * danxieai258@163.com
 */
public class HBase114SearchBuilder extends AbstractSearchBuilder {
    /**
     * 标识主键，表示hbase rowkey
     */
    private static final String primary_key = "rowkey";

    private String tableName;

    /**构建的query对象*/
    private Query query;

    /**返回的列*/
    private List<SelectExpressionItem> selectItem;

    private long limit = 1;

    @Override
    public String primaryKey() {
        return primary_key;
    }

    @Override
    public void limit(long limit) {
        if (limit > 0) {
           this.limit = limit;
        }
    }

    @Override
    public long getLimit() {
        return this.limit;
    }

    @Override
    public void builderQuery(Expression expression) {
        List<InnerExpression> list = new LinkedList<>();
        queryBuilder(list, expression, AND);
        ScanQuery scan = new ScanQuery(list, tableName, primaryKey() , limit, selectItem);
        query = scan;
    }


    @Override
    protected String selectItemLeftColumnName(Column column) {
        String columnName = column.getColumnName();
        if (primary_key.equals(columnName)) {
            return primary_key;
        }
        net.sf.jsqlparser.schema.Table table = column.getTable();
        Asserts.notNull(table, "you mast specify family name in you where conditions");
        return table.getName() + "." + columnName;

    }


    @Override
    public void builderGet(Expression expression) {
        GetQuery getQuery;
        EqualsTo equalsTo = (EqualsTo)expression;
        Expression rightExpression = equalsTo.getRightExpression();
        getQuery = new GetQuery(tableName, primaryKey(), selectItem);
        if (rightExpression instanceof JdbcParameter) {
            // JdbcParameter stringValue = ((JdbcParameter) ((LikeExpression) expression).getRightExpression());
        } else if (rightExpression instanceof StringValue) {
            StringValue stringValue = ((StringValue) equalsTo.getRightExpression());
            getQuery.idValue(stringValue.getValue());
        } else {
            throw new RuntimeException("not sport value, that not inside of StringValue, JDBCParameter (?)");
        }
        query = getQuery;
    }

    @Override
    public void setSelectItems(List<SelectExpressionItem> items) {
        this.selectItem = items;
    }

    @Override
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }



    public List<Map<String, String>> query(Connection connection, Object... objects) throws Exception{
        return query.query(connection, objects);
    }


    public abstract static class Query {

        List<InnerExpression> expressions;

        String tableName;

        String primaryKey;

        long limit;
        // key = family
        Map<String, Map<String, SelectItem>> item;

        public Query(List<InnerExpression> expressions, String tableName, String primaryKey, long limit, List<SelectExpressionItem> item) {
            this.expressions = expressions;
            this.tableName = tableName;
            this.primaryKey = primaryKey;
            this.limit = limit;
            this.item = new HashMap<>();
            setItem(item);
        }


        public Query() {}

        abstract List<Map<String, String>> query(Connection connection, Object... args) throws Exception;

        protected void setItem(List<SelectExpressionItem> item) {
            if (item != null && !item.isEmpty()) {
                for (SelectExpressionItem expressionItem : item) {
                    String queryRow = expressionItem.getExpression().toString();
                    Asserts.check(!queryRow.contains("\\."), "select row must like `family`.qualifier");
                    String ori = queryRow;
                    queryRow = queryRow.replaceAll("`", "");
                    String[] familyAndQualifier = queryRow.split("\\.");
                    SelectItem selectItem = new SelectItem(familyAndQualifier[0],
                            familyAndQualifier[1], ori);
                    if (expressionItem.getAlias() != null) {
                        selectItem.alias = expressionItem.getAlias().getName();
                    }
                    if (!this.item.containsKey(familyAndQualifier[0])) {
                        Map<String, SelectItem> map = new HashMap<>();
                        map.put(selectItem.column, selectItem);
                        this.item.put(selectItem.family, map);
                    } else {
                        Map<String, SelectItem> selectItemMap = this.item.get(familyAndQualifier[0]);
                        selectItemMap.put(selectItem.column, selectItem);
                    }
                }
            }
        }

        protected Map<String, String> setResultToMap(Result rs) {
            Map<String, String> map = new HashMap<>();
            List<Cell> cells = rs.listCells();
            if (cells == null || cells.isEmpty()) {
                return map;
            }
            for (Cell cell : cells) {
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String cf = Bytes.toString(CellUtil.cloneFamily(cell));
                String cq = Bytes.toString(CellUtil.cloneQualifier(cell));
                String val = Bytes.toString(CellUtil.cloneValue(cell));
                map.put(primaryKey, row);
                if (item.containsKey(cf) && item.get(cf).containsKey(cq)) {
                    SelectItem selectItem = item.get(cf).get(cq);
                    map.put(selectItem.alias == null ? selectItem.originalSelectRow : selectItem.alias,
                            val);
                }
            }
            return map;
        }

        public static class SelectItem {
            public String family;
            public String column;
            public String originalSelectRow;
            public String alias;

            public SelectItem(String family, String column, String originalSelectRow) {
                this.family = family;
                this.column = column;
                this.originalSelectRow = originalSelectRow;
            }
        }
    }


    public class ScanQuery extends Query {

        public ScanQuery(List<InnerExpression> expressions, String tableName, String primaryKey, long limit, List<SelectExpressionItem> item) {
            super(expressions, tableName, primaryKey, limit, item);
        }

        @Override
        public List<Map<String, String>> query(Connection connection, Object... args) throws Exception {
            List<Map<String, String>> resultList = new LinkedList<>();
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            int index = 0;
            FilterList filterList = new FilterList();
            for (InnerExpression expression : expressions) {
                String value = expression.right.toString();
                if (expression.isPlaceholder()) {
                    if (args == null || args.length <= index) {
                        throw new IllegalArgumentException("condition params are not match, check you SQL placeholder");
                    }
                    value = args[index].toString();
                }
                value = value.replace("%", "");
                if (expression.left.equals(primaryKey)) {
                    //我们目前只支持主键前缀匹配，并在SQL上使用 like。eg：select * from t where rowkey like "zhangsan%"
                    scan.setRowPrefixFilter(value.getBytes());
                } else {
                    String[] familyAndQualifier = expression.left.split("\\.");
                    CompareFilter.CompareOp compareOp = operateToCompareOp(expression.operate);
                    SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                            familyAndQualifier[0].trim().replaceAll("`", "").getBytes(),
                            familyAndQualifier[1].getBytes(),
                            compareOp, value.getBytes());
                    filterList.addFilter(singleColumnValueFilter);
                    /*if (compareOp.equals(CompareFilter.CompareOp.EQUAL)) {

                    } else {
                        CompareFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
                                ByteArrayComparable.parseFrom(familyAndQualifier[0].trim().
                                        replaceAll("`", "").getBytes()));
                        CompareFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                                ByteArrayComparable.parseFrom(familyAndQualifier[1].getBytes()));
                        CompareFilter valueFilter = new ValueFilter(compareOp, ByteArrayComparable.parseFrom(value.getBytes()));
                        filterList.addFilter(Arrays.asList(familyFilter, qualifierFilter, valueFilter));
                    }*/
                }
                index++;
            }
            PageFilter pagefilter = new PageFilter(limit);
            filterList.addFilter(pagefilter);
            scan.setFilter(filterList);
            // TODO 排序功能目前不支持
           /* if (orderByElements != null && !orderByElements.isEmpty()) {
                for(OrderByElement element : orderByElements) {

                }
            }*/
            ResultScanner results = table.getScanner(scan);
            for (Result rs : results) {
                Map<String, String> resultMap = setResultToMap(rs);
                if (!resultMap.isEmpty()) {
                    resultList.add(resultMap);
                }
            }
            return resultList;

        }

        private CompareFilter.CompareOp operateToCompareOp(String operate) {
            switch (operate) {
                case OperateEnum.EQUAL:
                    return CompareFilter.CompareOp.EQUAL;
                case OperateEnum.GREATER_THEN:
                    return CompareFilter.CompareOp.GREATER;
                case OperateEnum.LESS_THEN:
                    return CompareFilter.CompareOp.LESS;
                case OperateEnum.GREATER_THEN_EQUAL:
                    return CompareFilter.CompareOp.GREATER_OR_EQUAL;
                case OperateEnum.LESS_THEN_EQUAL:
                    return CompareFilter.CompareOp.LESS_OR_EQUAL;
                default:
                    throw new RuntimeException();
            }
        }
    }


    public static class GetQuery extends Query {

        private String idValue;

        public void idValue(String value) {
            this.idValue = value;
        }

        public GetQuery(String tableName, String primaryKey, List<SelectExpressionItem> selectItem) {
            this.tableName = tableName;
            this.primaryKey = primaryKey;
            this.item = new HashMap<>();
            setItem(selectItem);
        }

        @Override
        public List<Map<String, String>> query(Connection connection, Object... args) throws Exception {
            if (idValue == null && (args == null || args.length <= 0)) {
                throw new IllegalArgumentException("you must specify id value");
            }
            idValue = idValue == null ? args[0].toString() : idValue;
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(idValue));
            Result result = table.get(get);
            List<Map<String, String>> resultList = new LinkedList<>();
            Map<String, String> resultMap = setResultToMap(result);
            if (!resultMap.isEmpty()) {
                resultList.add(resultMap);
            }
            return resultList;
        }
    }

}
