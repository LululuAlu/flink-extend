package cn.lgwen.join;

import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * 2019/10/17
 * aven.wu
 * danxieai258@163.com
 * IN 输入类型
 * OUT 输出类型
 * TYPE 查询数据源返回的数据类型，一般Object
 */
public abstract class AbstractAsyncJoinFunction<IN, OUT, TYPE> extends RichAsyncFunction<IN, OUT> {

    protected final String SQL;

    protected List<String> matchField;

    protected BiFunction<IN, List<Map<String, TYPE>>, OUT> joinFunction;


    public AbstractAsyncJoinFunction(String SQL) {
        this.SQL = SQL;
    }

    public AbstractAsyncJoinFunction(String SQL, List<String> matchField, BiFunction<IN, List<Map<String, TYPE>>, OUT> joinFunction) {
        this.SQL = SQL;
        this.matchField = matchField;
        this.joinFunction = joinFunction;
    }
}
