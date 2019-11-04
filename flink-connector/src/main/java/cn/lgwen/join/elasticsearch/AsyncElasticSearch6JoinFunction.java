package cn.lgwen.join.elasticsearch;

import cn.lgwen.join.AbstractAsyncJoinFunction;
import cn.lgwen.sql.Elastic6SearchBuilder;
import cn.lgwen.sql.SqlParser;
import cn.lgwen.util.ReflectUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;


/**
 * 2019/10/15
 * aven.wu
 * danxieai258@163.com
 */
public class AsyncElasticSearch6JoinFunction<IN> extends AbstractAsyncJoinFunction<IN, IN, Object> {

    private transient RestHighLevelClient client;

    private HttpHost[] httpHosts;

    private transient Elastic6SearchBuilder searchBuilder;
    /**
     * ES 文档类型，doc type
     */
    private String type;


    public AsyncElasticSearch6JoinFunction(String SQL,
                                           List<String> matchField,
                                           BiFunction<IN, List<Map<String, Object>>, IN> joinFunction,
                                           HttpHost[] httpHosts) {
        super(SQL, matchField, joinFunction);
        this.httpHosts = httpHosts;
    }

    public AsyncElasticSearch6JoinFunction(String SQL,
                                           List<String> matchField,
                                           BiFunction<IN, List<Map<String, Object>>, IN> joinFunction,
                                           HttpHost[] httpHosts,
                                           String type) {
        super(SQL, matchField, joinFunction);
        this.httpHosts = httpHosts;
        this.type = type;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        client = new RestHighLevelClient(
                RestClient.builder(httpHosts)
        );
        searchBuilder = new Elastic6SearchBuilder();
        searchBuilder.type(type);
        SqlParser.parserSqlToBuilder(this.SQL, searchBuilder);
    }

    @Override
    public void close() throws Exception {
        super.close();
        client.close();
    }

    @Override
    public void asyncInvoke(IN input, ResultFuture<IN> resultFuture) throws Exception {
        List<Object> attribute = ReflectUtil.reflectObjectAttribute(matchField, input);
        if (attribute.size() != matchField.size()) {
            resultFuture.complete(Collections.singleton(input));
        } else {
            List<Map<String, Object>> query = searchBuilder.query(client, attribute.toArray(new Object[]{}));
            IN out = joinFunction.apply(input, query);
            resultFuture.complete(Collections.singleton(out));
        }
    }
}
