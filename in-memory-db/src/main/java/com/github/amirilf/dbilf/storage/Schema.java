package com.github.amirilf.dbilf.storage;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;

@Getter
public final class Schema {

    private final AtomicLong pkSequence = new AtomicLong(1);
    private final Field<?> pkField;
    private final Map<String, Field<?>> fields;

    private Schema(Map<String, Field<?>> fields, Field<?> pkField) {
        this.pkField = pkField;
        this.fields = Collections.unmodifiableMap(fields);
    }

    public Field<?> getPKField() {
        return pkField;
    }

    public Long getAndIncrement() {
        return pkSequence.getAndIncrement();
    }

    public static class Builder {

        private Field<?> pkField = null;
        private final Map<String, Field<?>> fields = new LinkedHashMap<>();

        public <T> Builder addField(String name, Class<T> type, boolean primaryKey, int maxLength) {
            if (fields.containsKey(name)) {
                throw new RuntimeException("Field " + name + " already exists in schema");
            }
            Field<?> field = new Field<>(name, type, primaryKey, maxLength);
            if (primaryKey) {
                if (pkField != null) {
                    throw new RuntimeException("Schema already has a primary key field");
                }
                pkField = field;
            }
            fields.put(name, field);
            return this;
        }

        public <T> Builder addField(String name, Class<T> type, int maxLength) {
            return addField(name, type, false, maxLength);
        }

        public <T> Builder addField(String name, Class<T> type, boolean primaryKey) {
            return addField(name, type, primaryKey, 0);
        }

        public <T> Builder addField(String name, Class<T> type) {
            return addField(name, type, false, 0);
        }

        public Schema build() {
            if (fields.containsKey("id") || fields.values().stream().anyMatch(Field::isPrimaryKey)) {
                throw new RuntimeException("There cannot be an 'id' field or an explicit primary key field");
            }
            addField("id", Long.class, true);
            return new Schema(fields, pkField);
        }
    }
}
