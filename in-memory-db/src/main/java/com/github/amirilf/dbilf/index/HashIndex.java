package com.github.amirilf.dbilf.index;

import com.github.amirilf.dbilf.storage.Row;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public final class HashIndex implements Index {

    private final String fieldName;
    private final boolean unique;
    private final ConcurrentHashMap<Object, CopyOnWriteArrayList<Row>> indexMap = new ConcurrentHashMap<>();

    public HashIndex(String fieldName, boolean unique) {
        this.fieldName = fieldName;
        this.unique = unique;
    }

    @Override
    public void insert(Row row) {
        Object value = row.getValue(fieldName);
        if (value == null) {
            throw new RuntimeException("Value for field " + fieldName + " cannot be null");
        }
        if (unique) {
            indexMap.compute(value, (k, list) -> {
                if (list != null && !list.isEmpty()) {
                    throw new RuntimeException("Duplicate value for unique index on field " + fieldName);
                }
                CopyOnWriteArrayList<Row> newList = new CopyOnWriteArrayList<>();
                newList.add(row);
                return newList;
            });
        } else {
            indexMap.compute(value, (k, list) -> {
                if (list == null) {
                    list = new CopyOnWriteArrayList<>();
                }
                list.add(row);
                return list;
            });
        }
    }

    @Override
    public void update(Row oldRow, Row newRow) {
        Object oldValue = oldRow.getValue(fieldName);
        Object newValue = newRow.getValue(fieldName);
        if (oldValue == null || newValue == null) {
            throw new RuntimeException("Value for field " + fieldName + " cannot be null");
        }
        if (!oldValue.equals(newValue)) {
            delete(oldRow);
            insert(newRow);
        } else {
            CopyOnWriteArrayList<Row> list = indexMap.get(newValue);
            if (list != null) {
                list.remove(oldRow);
                list.add(newRow);
            } else {
                throw new RuntimeException("Indexed value for " + newValue + " not found");
            }
        }
    }

    @Override
    public void delete(Row row) {
        Object value = row.getValue(fieldName);
        if (value == null) {
            throw new RuntimeException("Value for field " + fieldName + " cannot be null");
        }
        indexMap.computeIfPresent(value, (k, list) -> {
            list.remove(row);
            return list.isEmpty() ? null : list;
        });
    }

    @Override
    public List<Row> search(Object key) {
        CopyOnWriteArrayList<Row> list = indexMap.get(key);
        return list == null ? Collections.emptyList() : new ArrayList<>(list);
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public boolean isUnique() {
        return unique;
    }
}
