package com.github.amirilf.dbilf.storage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class Database {

    private static final Database INSTANCE = new Database();
    private final Map<String, Table> tables = new ConcurrentHashMap<>();

    private Database() {
    }

    public static Database getInstance() {
        return INSTANCE;
    }

    public void createTable(String tableName, Schema schema) {
        Table table = new Table(tableName, schema);
        if (tables.putIfAbsent(tableName, table) != null) {
            throw new RuntimeException("Table " + tableName + " already exists");
        }
    }

    public Table getTable(String tableName) {
        Table table = tables.get(tableName);
        if (table == null) {
            throw new RuntimeException("Table " + tableName + " does not exist");
        }
        return table;
    }

    public void deleteTable(String tableName) {
        if (tables.remove(tableName) == null) {
            throw new RuntimeException("Table " + tableName + " does not exist");
        }
    }
}
