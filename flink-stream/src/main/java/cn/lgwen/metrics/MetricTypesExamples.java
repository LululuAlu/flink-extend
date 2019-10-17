package cn.lgwen.metrics;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * 2019/10/11
 * aven.wu
 * danxieai258@163.com
 * document https://ci.apache.org/projects/flink/flink-docs-release-1.9/monitoring/metrics.html
 */
public class MetricTypesExamples {


    private static MapFunction<String, String> CounterMapper() {
        return new RichMapFunction<String, String>() {
            private transient Counter counter;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                this.counter = getRuntimeContext()
                        .getMetricGroup()
                        .counter("myCounter");
            }

            @Override
            public String map(String value) throws Exception {
                this.counter.inc();
                return value;
            }
        };
    }


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<String> list = Arrays.asList("zhangsan", "lisi", "wangwu");
        DataStreamSource<String> ds = env.fromCollection(list);
        ds.map(CounterMapper());
        env.execute();
    }



}
