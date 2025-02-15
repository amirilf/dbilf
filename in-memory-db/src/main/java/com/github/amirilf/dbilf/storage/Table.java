package com.github.amirilf.dbilf.storage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table {

    private final String name;
    private final Schema schema;
    private final Map<Object, Row> rows = new ConcurrentHashMap<>(); // key is pk
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public Table(String name, Schema schema) {
        this.name = name;
        this.schema = schema;
    }

    public String getName() {
        return name;
    }

    public Schema getSchema() {
        return schema;
    }

    public void insert(Row row) {
        lock.writeLock().lock();
        try {
            schema.validate(row.getData());
            Field<?> pkField = schema.getPrimaryKeyField(); // TODO: make sure there is pk in creating schema!
            Object key = row.getValue(pkField.getName());
            if (key == null)
                throw new RuntimeException("Primary key field '" + pkField.getName() + "' cannot be null");
            if (rows.containsKey(key))
                throw new RuntimeException("Duplicate primary key value: " + key);
            rows.put(key, row);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Row select(Object key) {
        lock.readLock().lock();
        try {
            return rows.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void update(Object key, Row newRow) {
        lock.writeLock().lock();
        try {
            schema.validate(newRow.getData());
            Field<?> pkField = schema.getPrimaryKeyField();
            Object newKey = newRow.getValue(pkField.getName());
            if (!key.equals(newKey))
                throw new RuntimeException("Primary key '" + key + "' doesn't match row's key '" + newKey + "'");
            if (!rows.containsKey(key))
                throw new RuntimeException("No result found with key: " + key);
            rows.put(key, newRow);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void delete(Object key) {
        lock.writeLock().lock();
        try {
            rows.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

}
