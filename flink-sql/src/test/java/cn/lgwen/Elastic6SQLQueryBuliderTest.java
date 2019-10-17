package cn.lgwen;

import cn.lgwen.sql.Elastic6SearchBuilder;
import cn.lgwen.sql.SqlParser;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * 2019/10/15
 * aven.wu
 * danxieai258@163.com
 */
public class Elastic6SQLQueryBuliderTest {

    RestHighLevelClient restHighLevelClient;

    @Before
    public void init() {
        restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("10.20.128.210",19201,"http"))
        );
    }


    @Test
    public void getIdForSpecifyValue()throws Exception {
        String sql = "select discoveryTime, cause, description from batch_dws_vul_info_201909 where _id = '9EC8F9CAB795C3CCE01BDA8E497C81D9'";
        Elastic6SearchBuilder searchBuilder = new Elastic6SearchBuilder();
        searchBuilder.type("data");
        SqlParser.parserSqlToBuilder(sql, searchBuilder);
        List<Map<String, Object>> res  = searchBuilder.query(restHighLevelClient, null);
        System.out.println(res);

    }

    @Test
    public void getId()throws Exception {
        String sql = "select discoveryTime, cause, description from batch_dws_vul_info_201909 where _id = ?";
        Elastic6SearchBuilder searchBuilder = new Elastic6SearchBuilder();
        searchBuilder.type("data");
        SqlParser.parserSqlToBuilder(sql, searchBuilder);
        List<Map<String, Object>> res = searchBuilder.query(restHighLevelClient, "9EC8F9CAB795C3CCE01BDA8E497C81D9");
        System.out.println(res);

    }

    @Test
    public void searchFieldCondition()throws Exception {
        String sql = "select city, collectId, description from `dwd_event.2019-10-08` where  city = ? ";
        Elastic6SearchBuilder searchBuilder = new Elastic6SearchBuilder();
        searchBuilder.type("data");
        SqlParser.parserSqlToBuilder(sql, searchBuilder);
        List<Map<String, Object>> res = searchBuilder.query(restHighLevelClient, "衡阳");
        System.out.println(res);
        Assert.assertEquals(res.get(0).get("city"), "衡阳");

    }

    @Test
    public void searchMultiFieldAndCondition()throws Exception {
        String sql = "select city, collectId, description from `dwd_event.2019-10-08` where  `city.keyword` = ? AND collectId = ?";
        Elastic6SearchBuilder searchBuilder = new Elastic6SearchBuilder();
        searchBuilder.type("data");
        SqlParser.parserSqlToBuilder(sql, searchBuilder);
        List<Map<String, Object>> res = searchBuilder.query(restHighLevelClient, "鞍山", "cloud");
        System.out.println(res);
        Assert.assertEquals(res.get(0).get("city"), "鞍山");

    }

    @Test
    public void searchLimit()throws Exception {
        String sql = "select discoveryTime, cause, description from `dwd_event.2019-10-08` where  city = ? limit 5";
        Elastic6SearchBuilder searchBuilder = new Elastic6SearchBuilder();
        searchBuilder.type("data");
        SqlParser.parserSqlToBuilder(sql, searchBuilder);
        List<Map<String, Object>> res = searchBuilder.query(restHighLevelClient, "衡阳");
        System.out.println(res);
        Assert.assertEquals(res.size(), 5);
    }

    @Test
    public void searchGreatThanCondition()throws Exception {
        String sql = "select discoveryTime, cause, endTime from `dwd_event.2019-10-08` where endTime >= ?";
        Elastic6SearchBuilder searchBuilder = new Elastic6SearchBuilder();
        searchBuilder.type("data");
        SqlParser.parserSqlToBuilder(sql, searchBuilder);
        List<Map<String, Object>> res = searchBuilder.query(restHighLevelClient, 1570487407000L);
        System.out.println(res);
        Assert.assertTrue(((Long)res.get(0).get("endTime")) >= 1570487407000L);
    }


    @Test
    public void searchLessThanCondition()throws Exception {
        String sql = "select discoveryTime, cause, endTime from `dwd_event.2019-10-08` where endTime <= ?";
        Elastic6SearchBuilder searchBuilder = new Elastic6SearchBuilder();
        searchBuilder.type("data");
        SqlParser.parserSqlToBuilder(sql, searchBuilder);
        List<Map<String, Object>> res = searchBuilder.query(restHighLevelClient, 1570487409000L);
        System.out.println(res);
        Assert.assertTrue(((Long)res.get(0).get("endTime")) <= 1570487409000L);
    }

    @Test
    public void searchLikeCondition()throws Exception {
        String sql = "select dstUnitName, dstIp, endTime from `dwd_event.2019-10-08` where dstUnitName like ?";
        Elastic6SearchBuilder searchBuilder = new Elastic6SearchBuilder();
        searchBuilder.type("data");
        SqlParser.parserSqlToBuilder(sql, searchBuilder);
        List<Map<String, Object>> res = searchBuilder.query(restHighLevelClient, "鞍山市公安%");
        System.out.println(res);
        Assert.assertEquals(res.get(0).get("dstUnitName"),  "鞍山市公安局公安交通管理局");
    }



    @Test
    public void getIdAndColumnAlias()throws Exception {
        String sql = "select cause as cause, id, submitTime as time from `batch_dws_vul_info_201909` where _id = ?";
        Elastic6SearchBuilder searchBuilder = new Elastic6SearchBuilder();
        searchBuilder.type("data");
        SqlParser.parserSqlToBuilder(sql, searchBuilder);
        List<Map<String, Object>> res = searchBuilder.query(restHighLevelClient, "AD07DBE393F118E794ABA92749AF9DA5");
        System.out.println(res);
        Assert.assertEquals(res.get(0).get("cause"),  "设计错误");
    }


    @Test
    public void searchWithSort()throws Exception {
        String sql = "select age, name from user where name = ? order by age asc";
        Elastic6SearchBuilder searchBuilder = new Elastic6SearchBuilder();
        searchBuilder.type("data");
        SqlParser.parserSqlToBuilder(sql, searchBuilder);
        List<Map<String, Object>> res = searchBuilder.query(restHighLevelClient, "小红");
        System.out.println(res);
        Assert.assertEquals(res.get(0).get("age"),  22);
    }

}
