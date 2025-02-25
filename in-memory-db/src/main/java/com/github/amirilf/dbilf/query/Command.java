package com.github.amirilf.dbilf.query;

import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Command {
    private CommandType type;
    private String tableName;
    private Map<String, Object> columnDefinitions; // for CREATE TABLE: column name -> [type, maxLength]
    private List<String> selectColumns; // for SELECT (if not "*")
    private Map<String, Object> insertValues; // for INSERT
    private Map<String, Object> updateValues; // for UPDATE
    private String conditionColumn; // for WHERE clause (single condition)
    private Object conditionValue;
    private String indexField; // for CREATE/DROP INDEX
    private boolean uniqueIndex; // for CREATE INDEX
}
