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
            if (indexMap.containsKey(value)) {
                throw new RuntimeException("Duplicate value for unique index on field " + fieldName);
            }
            CopyOnWriteArrayList<Row> list = new CopyOnWriteArrayList<>();
            list.add(row);
            indexMap.put(value, list);
        } else {
            indexMap.compute(value, (k, list) -> {
                if (list == null)
                    list = new CopyOnWriteArrayList<>();
                list.add(row);
                return list;
            });
        }
    }

    @Override
    public void update(Row oldRow, Row newRow) {
        // we sure oldValue already existed in our database
        Object oldValue = oldRow.getValue(fieldName);
        Object newValue = newRow.getValue(fieldName);
        if (newValue == null || oldValue == null) {
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
            } else
                // we expect to have such key in our index, cuz it's the same with old one
                throw new RuntimeException("There is no indexed value for: " + newValue);
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
            return list.isEmpty() ? null : list; // no need at all but anyway
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
