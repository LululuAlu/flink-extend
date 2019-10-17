package cn.lgwen.table;

import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
public class AggResult implements Serializable {

    private Long pvcount;

    private Timestamp processTime;

    private String gender;
}
