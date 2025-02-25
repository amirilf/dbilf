package com.github.amirilf.dbilf.storage;

import com.github.amirilf.dbilf.index.HashIndex;
import com.github.amirilf.dbilf.index.Index;
import com.github.amirilf.dbilf.transaction.TransactionManager;
import com.github.amirilf.dbilf.transaction.Transaction;
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
    private final ReentrantReadWriteLock tableLock = new ReentrantReadWriteLock();
    private final ConcurrentHashMap<Long, ReentrantReadWriteLock> rowLocks = new ConcurrentHashMap<>();

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
        tableLock.readLock().lock();
        try {
            return Collections.unmodifiableList(new ArrayList<>(rows.values()));
        } finally {
            tableLock.readLock().unlock();
        }
    }

    public void addIndex(String fieldName, boolean unique) {
        tableLock.writeLock().lock();
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
            tableLock.writeLock().unlock();
        }
    }

    public void removeIndex(String fieldName) {
        tableLock.writeLock().lock();
        try {
            if (!indexes.containsKey(fieldName)) {
                throw new RuntimeException("Index on field " + fieldName + " does not exist");
            }
            if (schema.getPKField().getName().equals(fieldName)) {
                throw new RuntimeException("Cannot remove primary key index");
            }
            indexes.remove(fieldName);
        } finally {
            tableLock.writeLock().unlock();
        }
    }

    public void create(Row row) {
        Long key = (Long) row.getValue(schema.getPKField().getName());
        ReentrantReadWriteLock lock = rowLocks.computeIfAbsent(key, k -> new ReentrantReadWriteLock());
        ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        writeLock.lock();
        boolean lockRegistered = false;
        try {
            if (rows.containsKey(key)) {
                throw new RuntimeException("Duplicate primary key: " + key);
            }
            rows.put(key, row);
            indexes.values().forEach(index -> index.insert(row));
            Transaction tx = TransactionManager.getCurrentTransaction();
            if (tx != null) {
                tx.register(() -> {
                    rows.remove(key);
                    rowLocks.remove(key);
                    indexes.values().forEach(index -> index.delete(row));
                });
                tx.registerLockRelease(() -> writeLock.unlock());
                lockRegistered = true;
            }
        } finally {
            if (!lockRegistered) {
                writeLock.unlock();
            }
        }
    }

    public List<Row> read(Object key, String fieldName) {
        if (!schema.getFields().containsKey(fieldName)) {
            throw new RuntimeException("Field " + fieldName + " does not exist in schema");
        }
        Class<?> fieldType = schema.getFields().get(fieldName).getType();
        if (!fieldType.isInstance(key)) {
            throw new RuntimeException("Key type does not match field: " + fieldName);
        }
        if (schema.getPKField().getName().equals(fieldName)) {
            Long pkKey = (Long) key;
            ReentrantReadWriteLock lock = rowLocks.get(pkKey);
            if (lock == null) {
                return Collections.emptyList();
            }
            ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
            readLock.lock();
            boolean lockRegistered = false;
            try {
                Row row = rows.get(pkKey);
                if (row == null) {
                    return Collections.emptyList();
                }
                Transaction tx = TransactionManager.getCurrentTransaction();
                if (tx != null) {
                    tx.registerLockRelease(() -> readLock.unlock());
                    lockRegistered = true;
                }
                return Collections.singletonList(row);
            } finally {
                if (!lockRegistered) {
                    readLock.unlock();
                }
            }
        } else {
            Index index = indexes.get(fieldName);
            List<Row> results;
            if (index != null) {
                results = index.search(key);
            } else {
                results = scanNonIndexed(fieldName, key);
            }
            Transaction tx = TransactionManager.getCurrentTransaction();
            if (tx != null) {
                results.forEach(row -> {
                    Long pk = (Long) row.getValue(schema.getPKField().getName());
                    ReentrantReadWriteLock rowLock = rowLocks.get(pk);
                    if (rowLock != null) {
                        ReentrantReadWriteLock.ReadLock rLock = rowLock.readLock();
                        rLock.lock();
                        tx.registerLockRelease(() -> rLock.unlock());
                    }
                });
            }
            return results;
        }
    }

    public void update(Row newRow) {
        Long key = (Long) newRow.getValue(schema.getPKField().getName());
        ReentrantReadWriteLock lock = rowLocks.get(key);
        if (lock == null)
            throw new RuntimeException("Row not found");
        ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        writeLock.lock();
        boolean lockRegistered = false;
        try {
            Row oldRow = rows.get(key);
            indexes.values().forEach(index -> index.update(oldRow, newRow));
            rows.put(key, newRow);
            Transaction tx = TransactionManager.getCurrentTransaction();
            if (tx != null) {
                tx.register(() -> {
                    rows.put(key, oldRow);
                    indexes.values().forEach(index -> index.update(newRow, oldRow));
                });
                tx.registerLockRelease(() -> writeLock.unlock());
                lockRegistered = true;
            }
        } finally {
            if (!lockRegistered) {
                writeLock.unlock();
            }
        }
    }

    public void delete(Long key) {
        ReentrantReadWriteLock lock = rowLocks.get(key);
        if (lock == null)
            throw new RuntimeException("Row not found");
        ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        writeLock.lock();
        boolean lockRegistered = false;
        try {
            Row oldRow = rows.get(key);
            rows.remove(key);
            indexes.values().forEach(index -> index.delete(oldRow));
            Transaction tx = TransactionManager.getCurrentTransaction();
            if (tx != null) {
                tx.register(() -> {
                    rows.put(key, oldRow);
                    indexes.values().forEach(index -> index.insert(oldRow));
                });
                tx.registerLockRelease(() -> writeLock.unlock());
                lockRegistered = true;
            }
        } finally {
            if (!lockRegistered) {
                writeLock.unlock();
            }
        }
    }

    private List<Row> scanNonIndexed(String fieldName, Object key) {
        List<Row> result = new ArrayList<>();
        for (Row row : rows.values()) {
            Object val = row.getValue(fieldName);
            if (val != null && val.equals(key)) {
                result.add(row);
            }
        }
        return result;
    }
}
