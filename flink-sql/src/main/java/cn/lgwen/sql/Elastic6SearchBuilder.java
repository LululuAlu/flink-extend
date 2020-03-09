package cn.lgwen.sql;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import org.apache.http.util.Asserts;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 2019/10/15
 * aven.wu
 * danxieai258@163.com
 */
public class Elastic6SearchBuilder extends AbstractSearchBuilder {

    public static final String primaryKey = "_id";

    private String index;

    // 7.0之后淘汰Type 我们也不做支持
    private String type;

    private long limit = 1;

    private List<SelectExpressionItem> items;

    private Query query;

    public String type() {

        return this.type;
    }

    @Override
    public String primaryKey() {
        return primaryKey;
    }

    @Override
    public void limit(long limit) {
        this.limit = limit;
    }

    @Override
    public long getLimit() {
        return limit;
    }

    @Override
    public void builderQuery(Expression expression) {
        if(type == null) {
            throw new IllegalArgumentException("you must set ES type before use SQL parser");
        }
        List<AbstractSearchBuilder.InnerExpression> list = new LinkedList<>();
        queryBuilder(list, expression, AND);
        SearchQuery scan = new SearchQuery(index, type, primaryKey(), limit, list, items);
        query = scan;
    }

    @Override
    public void builderGet(Expression expression) {
        if(type == null) {
            throw new IllegalArgumentException("you must set ES type before use SQL parser");
        }
        GetQuery getQuery;
        EqualsTo equalsTo = (EqualsTo)expression;
        Expression rightExpression = equalsTo.getRightExpression();
        if (rightExpression instanceof JdbcParameter) {
            // JdbcParameter stringValue = ((JdbcParameter) ((LikeExpression) expression).getRightExpression());
            getQuery = new GetQuery(index, type, items);
        } else if (rightExpression instanceof StringValue) {
            StringValue stringValue = ((StringValue) equalsTo.getRightExpression());
            getQuery = new GetQuery(index, type, items);
            getQuery.idValue(stringValue.getValue());
        } else {
            throw new RuntimeException("not sport value, that not inside of StringValue, JDBCParameter (?)");
        }
        query = getQuery;
    }

    @Override
    public void setSelectItems(List<SelectExpressionItem> items) {
        this.items = items;
    }

    @Override
    public void setTableName(String tableName) {
        this.index = tableName.replaceAll("`", "");
    }

    public List<Map<String, Object>> query(RestHighLevelClient client, Object... objects) throws Exception {
        return query.query(client, objects);
    }

    public void type(String type) {
        this.type = type;
    }

    @Override
    protected String selectItemLeftColumnName(Column column) {
        return column.getColumnName().replaceAll("`", "");
    }

    public abstract class Query {
        String index;
        String type;
        String primaryKey;
        long limit;
        List<InnerExpression> expressions;
        List<SelectExpressionItem> selectItem;

        public Query(String index, String type, String primaryKey, long limit,
                     List<InnerExpression> expressions,
                     List<SelectExpressionItem> selectItem) {
            this.index = index;
            this.type = type;
            this.primaryKey = primaryKey;
            this.expressions = expressions;
            this.selectItem = selectItem;
            this.limit = limit;
        }

        public Query() {
        }

        abstract List<Map<String, Object>> query(RestHighLevelClient client, Object... args) throws Exception;

        /**
         *
         * @param returnRs 需要获取的返回值
         * @param searchRs es查询的返回值
         */
        protected void setValue(Map<String, Object> returnRs, Map<String, Object> searchRs) {
            for (SelectExpressionItem element : selectItem) {
                String column = element.getExpression().toString();
                column = column.replaceAll("`", "");
                String alias = null;
                if (element.getAlias() != null) {
                    alias = element.getAlias().getName();
                }
                if (searchRs.containsKey(column)) {
                    returnRs.put(alias == null ? column : alias, searchRs.get(column));
                }
            }
        }
    }


    public class GetQuery extends Query {

        private String idValue;

        public GetQuery(String index, String type, List<SelectExpressionItem> selectItem) {
            this.index = index;
            this.type = type;
            this.selectItem = selectItem;
        }

        public void idValue(String value) {
            this.idValue = value;
        }


        @Override
        List<Map<String, Object>> query(RestHighLevelClient client, Object... args) throws Exception {
            if (idValue == null && (args == null || args.length <= 0)) {
                throw new IllegalArgumentException("you must specify id value");
            }
            GetRequest request = new GetRequest(index, type, idValue == null ? args[0].toString() : idValue);
            GetResponse response = client.get(request, RequestOptions.DEFAULT);
            Map<String, Object> searchRs = response.getSourceAsMap();
            Map<String, Object> returnRs = new HashMap<>();
            if (searchRs != null) {
                setValue(returnRs, searchRs);
            }
            if (returnRs.isEmpty()) {
                return new LinkedList<>();
            }
            return Collections.singletonList(returnRs);
        }
    }

    public class SearchQuery extends Query{

        public SearchQuery(String index, String type,
                           String primaryKey,
                           long limit,
                           List<InnerExpression> expressions,
                           List<SelectExpressionItem> selectItem) {
            super(index, type, primaryKey,limit, expressions, selectItem);
        }

        @Override
        List<Map<String, Object>> query(RestHighLevelClient client, Object... args) throws Exception {
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            List<String> items = selectItem.stream().map(x -> x.getExpression().toString().replaceAll("`", "")).collect(Collectors.toList());
            sourceBuilder.fetchSource(items.toArray(new String[]{}),null);
            sourceBuilder.from(0);
            sourceBuilder.size((int) limit);
            if (expressions.size() == 1) {
                //单条件查询
                InnerExpression expression = expressions.get(0);
                Object right = expression.right;
                if (expression.isPlaceholder()) {
                    Asserts.check(args != null && args.length >= 1, "args number not operates count");
                    if (args[0] instanceof String) {
                        right = args[0].toString().replaceAll("%", "");
                    } else {
                        right = args[0];
                    }
                }
                QueryBuilder queryBuilder = queryBuilder(expression, right);
                sourceBuilder.query(queryBuilder);
            } else if (expressions.size() > 1){
                BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                int idx = 0;
                for (InnerExpression expression : expressions) {
                    Object right = expression.right;
                    if (expression.isPlaceholder()) {
                        Asserts.check(args != null && args.length >= 1, "args number not operates count");
                        right = args[idx++];
                    }
                    if (expression.condition.equals(OR)) {
                        boolQueryBuilder.should(queryBuilder(expression, right));
                    } else {
                        if (expression.operate.equals("!=")) {
                            boolQueryBuilder.mustNot(queryBuilder(expression, right));
                        } else {
                            boolQueryBuilder.must(queryBuilder(expression, right));
                        }
                    }
                }
                sourceBuilder.query(boolQueryBuilder);
            } else {
                //TODO no condition
            }
            SearchRequest searchRequest = new SearchRequest(index);
            searchRequest.types(type);
            searchRequest.source(sourceBuilder);
            //设置排序
            if (orderByElements != null && !orderByElements.isEmpty()) {
                for(OrderByElement element : orderByElements) {
                    Column column = (Column)element.getExpression();
                    // TODO 目前不支持指定tableName
                    //String table = column.getTable().getName();
                    String columnName = column.getColumnName();
                    SortOrder sortOrder;
                    if (element.isAsc()) {
                        sortOrder = SortOrder.ASC;
                    } else {
                        sortOrder = SortOrder.DESC;
                    }
                    sourceBuilder.sort(columnName, sortOrder);
                }
            }
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            List<Map<String, Object>> result = new LinkedList<>();
            if (response.getHits().totalHits > 0) {
                for (SearchHit hit : response.getHits()) {
                    if (hit.hasSource()) {
                        Map<String, Object> returnRs = new HashMap<>();
                        setValue(returnRs, hit.getSourceAsMap());
                        if (!returnRs.isEmpty()) {
                            result.add(returnRs);
                        }
                    }
                }
            }
            return result;
        }


        private QueryBuilder queryBuilder(InnerExpression expression, Object arg) {
            switch (expression.operate){
                case OperateEnum.EQUAL:
                    return QueryBuilders.matchQuery(expression.left, arg);
                case OperateEnum.LESS_THEN:
                    return QueryBuilders.rangeQuery(expression.left).lt(arg);
                case OperateEnum.LESS_THEN_EQUAL:
                    return QueryBuilders.rangeQuery(expression.left).lte(arg);
                case OperateEnum.GREATER_THEN:
                    return QueryBuilders.rangeQuery(expression.left).gt(arg);
                case OperateEnum.GREATER_THEN_EQUAL:
                    return QueryBuilders.rangeQuery(expression.left).gte(arg);
                case OperateEnum.LIKE:
                    return QueryBuilders.fuzzyQuery(expression.left, arg);
                default:
                    throw new IllegalArgumentException("not sport operate");
            }
        }
    }



}
