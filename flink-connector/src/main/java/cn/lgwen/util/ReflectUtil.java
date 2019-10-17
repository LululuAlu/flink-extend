package cn.lgwen.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 2019/10/16
 * aven.wu
 * danxieai258@163.com
 */
public class ReflectUtil {

    /**
     * get specify values from object
     * @param fieldNames get specify fieldNames
     * @param o targetObject JAVA POJO. caution：object class must implement getter method
     * @return
     */
    public static List<Object> reflectObjectAttribute (List<String> fieldNames, Object o) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        Class clazz = o.getClass();
        List<String> methodNames =
                fieldNames.stream().map(ReflectUtil::toGetter).collect(Collectors.toList());

        List<Object> values = new LinkedList<>();
        for (String methodName : methodNames) {
            Method method = clazz.getMethod(methodName);
            Object fieldValue = method.invoke(o);
            values.add(fieldValue);
        }
        return values;
    }

    public static String toGetter(String fieldName) {
        return toGetterOrSetter("get", fieldName);
    }

    public static String toSetter(String fieldName) {
        return toGetterOrSetter("set", fieldName);
    }



    public static String toGetterOrSetter(String methodPerfix, String fieldName) {
        if (fieldName == null || fieldName.length() == 0) {
            return null;
        }
        /* If the second char is upper, make 'get' + field name as getter name. For example, eBlog -> geteBlog */
        if (fieldName.length() > 2) {
            String second = fieldName.substring(1, 2);
            if (second.equals(second.toUpperCase())) {
                return new StringBuffer(methodPerfix).append(fieldName).toString();
            }
        }
        /* Common situation */
        fieldName = new StringBuffer(methodPerfix).append(fieldName.substring(0, 1).toUpperCase())
                .append(fieldName.substring(1)).toString();

        return fieldName;
    }
}
