package cn.lgwen.table;


import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

@Data
@NoArgsConstructor
public class User {

    public User(Integer id, String name, String gender) {
        this.id = id;
        this.name = name;
        this.gender = gender;
    }

    private Integer id;

    private String name;

    private Timestamp birth;

    private String gender;

}
