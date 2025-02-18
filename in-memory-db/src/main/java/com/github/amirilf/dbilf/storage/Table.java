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
    private final ConcurrentHashMap<Long, Row> rows = new ConcurrentHashMap<>(); // default storage using `id` as key
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
        return indexes;
    }

    public List<Row> getRows() {
        lock.readLock().lock();
        try {
            return Collections.unmodifiableList(new ArrayList<>(rows.values()));
        } finally {
            lock.readLock().unlock();
        }
    }

    // ===== INDEX
    public void addIndex(String fieldName, boolean unique) {
        lock.writeLock().lock();
        try {
            if (indexes.containsKey(fieldName) || fieldName.equals("id")) {
                throw new RuntimeException("Index on field " + fieldName + " already exists");
            }

            if (schema.getFields().get(fieldName) == null) {
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
            if (schema.getPKField().getName().equals(fieldName)) { // in our case `id`
                throw new RuntimeException("Can't remove primary key index");
            }
            indexes.remove(fieldName);
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ===== CRUD
    public void create(Row row) {
        lock.writeLock().lock();
        try {
            Field<?> pk = schema.getPKField();
            Long key = (Long) row.getValue(pk.getName());
            if (key == null) {
                throw new RuntimeException("Primary key field '" + pk.getName() + "' cannot be null");
            }
            Row previous = rows.putIfAbsent(key, row);
            if (previous != null) {
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
            if (fieldType != key.getClass()) {
                throw new RuntimeException("The key has not the same type with the field: `" + fieldName
                        + "` which is " + fieldType.getSimpleName());
            }

            if (schema.getPKField().getName().equals(fieldName)) {
                return read(key);
            }

            // try to find using index
            Index index = indexes.get(fieldName);
            if (index != null) {
                return index.search(key);
            }

            // handy search
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

    // read by primary key.
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
            Long newID = (Long) newRow.getValue(schema.getPKField().getName());
            Row oldRow = rows.get(newID);
            if (oldRow == null) {
                throw new RuntimeException("No record found to update");
            }
            if (oldRow.equals(newRow)) {
                throw new RuntimeException("There is no change to update");
            }
            rows.compute(newID, (k, row) -> {
                if (row == null) {
                    throw new RuntimeException("No row found with key: " + row);
                }
                // Update all indexes atomically relative to the row update.
                indexes.values().forEach(index -> index.update(row, newRow));
                return newRow;
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void delete(Long key) {
        lock.writeLock().lock();
        try {
            rows.compute(key, (k, oldRow) -> {
                if (oldRow == null) {
                    throw new RuntimeException("No row found with key: " + key);
                }
                indexes.values().forEach(index -> index.delete(oldRow));
                return null;
            });
        } finally {
            lock.writeLock().unlock();
        }
    }
}