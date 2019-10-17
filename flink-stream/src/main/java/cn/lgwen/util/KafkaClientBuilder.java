package cn.lgwen.util;

/**
 * 2019/9/23
 * aven.wu
 * danxieai258@163.com
 */
public class KafkaClientBuilder {


    public static class Builder {

        private String bootstarpServers;

        private String batchSize;

        private String maxRequestSize;

        public Builder bootstrapServers() {
            return this;
        }
    }
}
