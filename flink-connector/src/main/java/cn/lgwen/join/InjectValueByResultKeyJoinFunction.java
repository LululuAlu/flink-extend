package cn.lgwen.join;

import cn.lgwen.util.ReflectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * 2019/10/16
 * aven.wu
 * danxieai258@163.com
 * 根据返回的 查询结果的字段名 注入对象对应的属性值 key = attribute
 * TYPE query resultType
 */
public class InjectValueByResultKeyJoinFunction<IN, OUT, TYPE> implements BiFunction<IN, List<Map<String, TYPE>>, OUT>, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(InjectValueByResultKeyJoinFunction.class);

    private transient static DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public OUT apply(IN in, List<Map<String, TYPE>> maps) {
        //根据返回值的key 设置到给定对象
        Map<String, TYPE> rs = maps.get(0);
        Class clazz = in.getClass();
        for (String methodName : rs.keySet()) {
            Method getMethod;
            try {
                getMethod = clazz.getMethod(ReflectUtil.toGetter(methodName));
            } catch (NoSuchMethodException e) {
                logger.debug("count reflect method by mapKey {}, {}", methodName, e.getMessage());
                continue;
            }
            try {
                Class returnType = getMethod.getReturnType();
                Object setValue = exchangeValue(returnType, rs.get(methodName));
                Method setMethod = clazz.getMethod(ReflectUtil.toSetter(methodName), returnType);
                setMethod.invoke(in, setValue);
            } catch (Exception e) {
                throw new RuntimeException(String.format("map key [%s] .", methodName) + e.getMessage());
            }
        }
        return (OUT) in;
    }

    private Object exchangeValue(Class returnType, TYPE value) {
        if (Integer.class == returnType) {
            if (value instanceof String) {
                return Integer.valueOf((String) value);
            }
            if (value instanceof Integer) {
                return value;
            }
        } else if (Long.class == returnType) {
            if (value instanceof String) {
                return Long.valueOf((String) value);
            }
            if (value instanceof Long) {
                return value;
            }
        } else if (Short.class == returnType) {
            if (value instanceof String) {
                return Short.valueOf((String) value);
            }
            if (value instanceof Short) {
                return value;
            }
        } else if (Character.class == returnType) {
            if (value instanceof String) {
                return ((String) value).toCharArray()[0];
            }
            if (value instanceof Character) {
                return value;
            }
        } else if (Float.class == returnType) {
            if (value instanceof String) {
                return Float.valueOf((String) value);
            }
            if (value instanceof Float) {
                return value;
            }
        } else if (Double.class == returnType) {
            if (value instanceof String) {
                return Double.valueOf((String) value);
            }
            if (value instanceof Double) {
                return value;
            }
        } else if (BigDecimal.class == returnType) {
            if (value instanceof String) {
                return new BigDecimal((String) value);
            }
            if (value instanceof BigDecimal) {
                return value;
            }
        } else if (Byte.class == returnType) {
            if (value instanceof String) {
                return ((String) value).getBytes()[0];
            }
            if (value instanceof Byte) {
                return value;
            }
        } else if (Integer.TYPE == returnType) {
            if (value instanceof String) {
                return Integer.valueOf((String) value);
            }
            if (value instanceof Integer) {
                return value;
            }
        } else if (Long.TYPE == returnType) {
            if (value instanceof String) {
                return Long.valueOf((String) value);
            }
            if (value instanceof Long) {
                return value;
            }
        } else if (Short.TYPE == returnType) {
            if (value instanceof String) {
                return Short.valueOf((String) value);
            }
            if (value instanceof Short) {
                return value;
            }
        } else if (Character.TYPE == returnType) {
            if (value instanceof String) {
                return ((String) value).toCharArray()[0];
            }
            if (value instanceof Character) {
                return value;
            }
        } else if (Float.TYPE == returnType) {
            if (value instanceof String) {
                return Float.valueOf((String) value);
            }
            if (value instanceof Float) {
                return value;
            }
        } else if (Double.TYPE == returnType) {
            if (value instanceof String) {
                return Double.valueOf((String) value);
            }
            if (value instanceof Double) {
                return value;
            }
        } else if (Byte.TYPE == returnType) {
            if (value instanceof String) {
                return ((String) value).getBytes()[0];
            }
            if (value instanceof Byte) {
                return value;
            }
        } else if (Timestamp.class == returnType) {
            if (value instanceof String) {
                try {
                    LocalDateTime time = LocalDateTime.parse((String) value, df);
                    return new Timestamp(time.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
                } catch (Exception e) {
                    return new Timestamp(Long.valueOf((String) value));
                }
            }
            if (value instanceof Timestamp) {
                return value;
            }
            if (value instanceof Date) {
                return new Timestamp(((Date) value).getTime());
            }

        } else if (Date.class == returnType) {
            if (value instanceof String) {
                try {
                    return new Date(Long.valueOf((String) value));
                } catch (Exception e) {
                    LocalDateTime time = LocalDateTime.parse((String) value, df);
                    return Date.from(time.atZone(ZoneId.systemDefault()).toInstant());
                }
            } if (value instanceof Date) {
                return value;
            }
        } else if (String.class == returnType) {
            return String.valueOf(value);
        }
        return value;
    }

}
