package cn.lgwen;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

/**
 * 2019/10/12
 * aven.wu
 * danxieai258@163.com
 */
public class JsonTest {

    @Test
    public void test() throws Exception{
        String json = "{\"name\":\"张三\",\"age\":10,\"gender\":1,\"fimaly\":[\"sun\",\"mom\",\"dad\"]}";

        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(json);
        System.out.println(node.findValue("name").asText());


    }
}
