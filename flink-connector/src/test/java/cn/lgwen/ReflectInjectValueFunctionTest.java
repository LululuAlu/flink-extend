package cn.lgwen;

import cn.lgwen.join.InjectValueByResultKeyJoinFunction;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 2019/10/17
 * aven.wu
 * danxieai258@163.com
 */
public class ReflectInjectValueFunctionTest {

    @Test
    public void attributeInjectTest() {
        InjectValueByResultKeyJoinFunction<User, User, String> function = new InjectValueByResultKeyJoinFunction<>();
        Map<String, String> map = new HashMap<>();
        map.put("age", "15");
        map.put("tall", "170");
        map.put("rowKey", "1453123");
        map.put("salary", "15k");
        User user = new User();
        function.apply(user, Collections.singletonList(map));
        System.out.println(user);
        Assert.assertEquals(user.getSalary(), "15k");
    }

    @Test
    public void reflectForTypesAttributeBasicType() {
        InjectValueByResultKeyJoinFunction<MultiBasicTypeAttr, MultiBasicTypeAttr, String> function = new InjectValueByResultKeyJoinFunction<>();
        Map<String, String> map = new HashMap<>();
        map.put("a", "1");
        map.put("b", "c");
        map.put("c", "123");
        map.put("d", "123");
        map.put("e", "1571273616000");
        map.put("f", "1.2");
        map.put("g", "13.21");
        MultiBasicTypeAttr attr = new MultiBasicTypeAttr();
        function.apply(attr, Collections.singletonList(map));
        System.out.println(attr);
    }


    @Test
    public void reflectForTypesAttributePackType() {
        InjectValueByResultKeyJoinFunction<PackTypeAttr, PackTypeAttr, String> function = new InjectValueByResultKeyJoinFunction<>();
        Map<String, String> map = new HashMap<>();
        map.put("a", "1");
        map.put("b", "c");
        map.put("c", "123");
        map.put("d", "123");
        map.put("e", "1571273616000");
        map.put("f", "1.2");
        map.put("g", "13.21");
        map.put("h", "12.1");
        map.put("i", "1571273616000");
        PackTypeAttr attr = new PackTypeAttr();
        function.apply(attr, Collections.singletonList(map));
        System.out.println(attr);
    }

    @Test
    public void reflectTypeOfDateTimeOfLongTimestamp() {
        InjectValueByResultKeyJoinFunction<DateTimeAttr, DateTimeAttr, String> function = new InjectValueByResultKeyJoinFunction<>();
        Map<String, String> map = new HashMap<>();
        map.put("timestamp", "1571273616000");
        map.put("date", "1571273616000");
        DateTimeAttr attr = new DateTimeAttr();
        function.apply(attr, Collections.singletonList(map));
        System.out.println(attr);
        Assert.assertEquals(1571273616000L, attr.date.getTime());
        Assert.assertEquals(1571273616000L, attr.timestamp.getTime());
    }

    @Test
    public void reflectTypeOfDateTimeOfStringDataFormat() {
        InjectValueByResultKeyJoinFunction<DateTimeAttr, DateTimeAttr, String> function = new InjectValueByResultKeyJoinFunction<>();
        Map<String, String> map = new HashMap<>();
        map.put("timestamp", "2017-10-05 12:21:01");
        map.put("date", "2017-10-05 12:21:01");
        DateTimeAttr attr = new DateTimeAttr();
        function.apply(attr, Collections.singletonList(map));
        System.out.println(attr);
        Assert.assertEquals(1507177261000L, attr.date.getTime());
        Assert.assertEquals(1507177261000L, attr.timestamp.getTime());
    }

    public static class DateTimeAttr {
        private Timestamp timestamp;
        private Date date;

        public Timestamp getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Timestamp timestamp) {
            this.timestamp = timestamp;
        }

        public Date getDate() {
            return date;
        }

        public void setDate(Date date) {
            this.date = date;
        }
    }

    public static class PackTypeAttr {
        private Byte a;
        private Character b;
        private Short c;
        private Integer d;
        private Long e;
        private Float f;
        private Double g;

        private BigDecimal h;


        public Byte getA() {
            return a;
        }

        public void setA(Byte a) {
            this.a = a;
        }

        public Character getB() {
            return b;
        }

        public void setB(Character b) {
            this.b = b;
        }

        public Short getC() {
            return c;
        }

        public void setC(Short c) {
            this.c = c;
        }

        public Integer getD() {
            return d;
        }

        public void setD(Integer d) {
            this.d = d;
        }

        public Long getE() {
            return e;
        }

        public void setE(Long e) {
            this.e = e;
        }

        public Float getF() {
            return f;
        }

        public void setF(Float f) {
            this.f = f;
        }

        public Double getG() {
            return g;
        }

        public void setG(Double g) {
            this.g = g;
        }

        public BigDecimal getH() {
            return h;
        }

        public void setH(BigDecimal h) {
            this.h = h;
        }
    }




    public static class MultiBasicTypeAttr {
        private byte a;

        private char b;

        private short c;

        private int d;

        private long e;

        private float f;

        private double g;

        @Override
        public String toString() {
            return "MultiTypeAttr{" +
                    "a=" + a +
                    ", b=" + b +
                    ", c=" + c +
                    ", d=" + d +
                    ", e=" + e +
                    ", f=" + f +
                    ", g=" + g +
                    '}';
        }

        public byte getA() {
            return a;
        }

        public void setA(byte a) {
            this.a = a;
        }

        public char getB() {
            return b;
        }

        public void setB(char b) {
            this.b = b;
        }

        public short getC() {
            return c;
        }

        public void setC(short c) {
            this.c = c;
        }

        public int getD() {
            return d;
        }

        public void setD(int d) {
            this.d = d;
        }

        public long getE() {
            return e;
        }

        public void setE(long e) {
            this.e = e;
        }

        public float getF() {
            return f;
        }

        public void setF(float f) {
            this.f = f;
        }

        public double getG() {
            return g;
        }

        public void setG(double g) {
            this.g = g;
        }
    }


}
