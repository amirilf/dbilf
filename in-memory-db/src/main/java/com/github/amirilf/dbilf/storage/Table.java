package com.github.amirilf.dbilf.storage;

import com.github.amirilf.dbilf.index.HashIndex;
import com.github.amirilf.dbilf.index.Index;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class Table {

    private final String name;
    private final Schema schema;
    private final Map<Long, Row> rows = new ConcurrentHashMap<>();
    private final Map<String, Index> indexes = new ConcurrentHashMap<>();
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

    public Map<String, Index> getIndexes() {
        return Collections.unmodifiableMap(indexes);
    }

    public List<Row> getRows() {
        lock.readLock().lock();
        try {
            return Collections.unmodifiableList(new ArrayList<>(rows.values()));
        } finally {
            lock.readLock().unlock();
        }
    }

    public void addIndex(String fieldName, boolean unique) {
        lock.writeLock().lock();
        try {
            if (indexes.containsKey(fieldName) || "id".equals(fieldName)) {
                throw new RuntimeException("Index on field " + fieldName + " already exists");
            }
            if (!schema.getFields().containsKey(fieldName)) {
                throw new RuntimeException("Field " + fieldName + " does not exist in schema");
            }
            HashIndex index = new HashIndex(fieldName, unique);
            rows.values().forEach(index::insert);
            indexes.put(fieldName, index);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removeIndex(String fieldName) {
        lock.writeLock().lock();
        try {
            if (!indexes.containsKey(fieldName)) {
                throw new RuntimeException("Index on field " + fieldName + " does not exist");
            }
            if (schema.getPKField().getName().equals(fieldName)) {
                throw new RuntimeException("Cannot remove primary key index");
            }
            indexes.remove(fieldName);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void create(Row row) {
        lock.writeLock().lock();
        try {
            Field<?> pk = schema.getPKField();
            Long key = (Long) row.getValue(pk.getName());
            if (key == null) {
                throw new RuntimeException("Primary key field '" + pk.getName() + "' cannot be null");
            }
            if (rows.putIfAbsent(key, row) != null) {
                throw new RuntimeException("Duplicate primary key value: " + key);
            }
            indexes.values().forEach(index -> index.insert(row));
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<Row> read(Object key, String fieldName) {
        lock.readLock().lock();
        try {
            if (!schema.getFields().containsKey(fieldName)) {
                throw new RuntimeException("Field " + fieldName + " does not exist in schema");
            }
            Class<?> fieldType = schema.getFields().get(fieldName).getType();
            if (!fieldType.isInstance(key)) {
                throw new RuntimeException("Key type does not match field: " + fieldName);
            }
            if (schema.getPKField().getName().equals(fieldName)) {
                return read(key);
            }
            Index index = indexes.get(fieldName);
            if (index != null) {
                List<Row> results = index.search(key);
                if (results.isEmpty()) {
                    throw new RuntimeException("No row found with key: " + key);
                }
                return results;
            }
            List<Row> result = new ArrayList<>();
            for (Row row : rows.values()) {
                Object val = row.getValue(fieldName);
                if (val != null && val.equals(key)) {
                    result.add(row);
                }
            }
            if (result.isEmpty()) {
                throw new RuntimeException("No row found with key: " + key);
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<Row> read(Object key) {
        lock.readLock().lock();
        try {
            Row row = rows.get(key);
            if (row == null) {
                throw new RuntimeException("No row found with key: " + key);
            }
            return Collections.singletonList(row);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void update(Row newRow) {
        lock.writeLock().lock();
        try {
            Long key = (Long) newRow.getValue(schema.getPKField().getName());
            Row oldRow = rows.get(key);
            if (oldRow == null) {
                throw new RuntimeException("No record found to update for key: " + key);
            }
            if (oldRow.equals(newRow)) {
                throw new RuntimeException("No changes detected for update");
            }
            indexes.values().forEach(index -> index.update(oldRow, newRow));
            rows.put(key, newRow);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void delete(Long key) {
        lock.writeLock().lock();
        try {
            Row oldRow = rows.get(key);
            if (oldRow == null) {
                throw new RuntimeException("No row found with key: " + key);
            }
            indexes.values().forEach(index -> index.delete(oldRow));
            rows.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
