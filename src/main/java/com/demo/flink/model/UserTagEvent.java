package com.demo.flink.model;

import lombok.Data;

/**
 * @author henry.fan
 */
@Data
public class UserTagEvent implements java.io.Serializable {
    public Long user_id;
    public String tag_code;
    public String tag_value;
    public String tag_type;
    public String tag_version;
    public String source_system;
    public String event_time; // 可后续转为 Timestamp
}

