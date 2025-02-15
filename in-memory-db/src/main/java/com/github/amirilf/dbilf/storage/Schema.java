package com.github.amirilf.dbilf.storage;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import lombok.Getter;

@Getter
public class Schema {

    private final Map<String, Field<?>> fields;

    private Schema(Map<String, Field<?>> fields) {
        this.fields = Collections.unmodifiableMap(fields); // immutable
    }

    // check the given row-data against the schema
    public void validate(Map<String, Object> rowData) {
        for (Map.Entry<String, Field<?>> entry : fields.entrySet()) {
            String fieldName = entry.getKey();
            Field<?> field = entry.getValue();
            Object value = rowData.get(fieldName);
            if (!field.validate(value)) {
                throw new RuntimeException("Invalid value for field '" + fieldName +
                        "'. Expected type: " + field.getType().getSimpleName() +
                        // if it's because of VARCHAR length
                        (field.getMaxLength() > 0 ? " (max length " + field.getMaxLength() + ")" : ""));
            }
        }
    }

    // return pk field if exists
    public Field<?> getPrimaryKeyField() {
        return fields.values().stream()
                .filter(Field::isPrimaryKey)
                .findFirst()
                .orElse(null);
    }

    // builder for creating schema
    public static class Builder {

        private final Map<String, Field<?>> fields = new LinkedHashMap<>();

        public <T> Builder addField(String name, Class<T> type, boolean primaryKey, int maxLength) {
            if (fields.containsKey(name)) {
                throw new RuntimeException("Field " + name + " already exists in schema");
            }
            fields.put(name, new Field<>(name, type, primaryKey, maxLength));
            return this;
        }

        // no pk
        public <T> Builder addField(String name, Class<T> type, int maxLength) {
            return addField(name, type, false, maxLength);
        }

        // no maxLength
        public <T> Builder addField(String name, Class<T> type, boolean primaryKey) {
            return addField(name, type, primaryKey, 0);
        }

        // no pk & maxLength
        public <T> Builder addField(String name, Class<T> type) {
            return addField(name, type, false, 0);
        }

        // if there is no pk, creates 'id' field as pk!
        public Schema build() {
            if (fields.values().stream().noneMatch(Field::isPrimaryKey)) {
                if (fields.containsKey("id")) {
                    throw new IllegalArgumentException("There can't be `id` field without being pk");
                }
                fields.put("id", new Field<>("id", Long.class, true, 0));
            }
            return new Schema(fields);
        }
    }

}
