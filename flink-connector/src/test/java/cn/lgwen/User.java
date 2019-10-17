package cn.lgwen;

import java.io.Serializable;

/**
 * 2019/10/17
 * aven.wu
 * danxieai258@163.com
 */
public class User implements Serializable {

    public User(String rowKey) {
        this.rowKey = rowKey;
    }

    public User() {
    }

    private String rowKey;

    private String name;

    private Integer age;

    private String salary;

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getSalary() {
        return salary;
    }

    public void setSalary(String salary) {
        this.salary = salary;
    }

    @Override
    public String toString() {
        return "User{" +
                "rowKey='" + rowKey + '\'' +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", salary='" + salary + '\'' +
                '}';
    }
}