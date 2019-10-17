package cn.lgwen.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 2019/10/11
 * aven.wu
 * danxieai258@163.com
 */
public class WindowJoin {

    public static void main(String[] args) {
        DataStream<Integer> orangeStream = null;
        DataStream<Integer> greenStream = null;

        orangeStream.join(greenStream)
                .where((x) -> x)
            .equalTo((x) -> x)
            .window(TumblingEventTimeWindows.of(Time.milliseconds(2)))
                .apply (new JoinFunction<Integer, Integer, String>(){
                    @Override
                    public String join(Integer first, Integer second) {
                        return first + "," + second;
                    }
                });
    }
}
