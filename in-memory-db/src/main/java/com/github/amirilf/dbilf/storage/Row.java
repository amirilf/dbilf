package com.github.amirilf.dbilf.storage;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import lombok.ToString;

@ToString
public class Row {

    private final Map<String, Object> data;

    public Row(Map<String, Object> data) {
        this.data = new HashMap<>(data);
    }

    public Object getValue(String fieldName) {
        return data.get(fieldName);
    }

    public Map<String, Object> getData() {
        return Collections.unmodifiableMap(data);
    }

}
