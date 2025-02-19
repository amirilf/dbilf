package com.github.amirilf.dbilf.storage;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public final class Row {

    private final Map<String, Object> data;

    private Row(Map<String, Object> data) {
        this.data = data;
    }

    public Object getValue(String fieldName) {
        return data.get(fieldName);
    }

    public Map<String, Object> getData() {
        return Collections.unmodifiableMap(data);
    }

    public static class Builder {

        private final Map<String, Object> data = new HashMap<>();
        private final Schema schema;
        private boolean customID = false;

        public Builder(Schema schema) {
            this.schema = schema;
        }

        public Builder set(String fieldName, Object value) {
            if ("id".equals(fieldName)) {
                throw new RuntimeException("The id field is managed automatically");
            }
            if (!schema.getFields().containsKey(fieldName)) {
                throw new RuntimeException("Field " + fieldName + " does not exist in schema");
            }
            data.put(fieldName, value);
            return this;
        }

        public Builder setId(Object value) {
            data.put("id", value);
            customID = true;
            return this;
        }

        public Row build() {
            if (!customID) {
                data.put("id", schema.getAndIncrement());
            }
            schema.getFields().forEach((fieldName, field) -> {
                if (!data.containsKey(fieldName)) {
                    throw new RuntimeException("Field " + fieldName + " is not set");
                }
                Object value = data.get(fieldName);
                if (!field.validate(value)) {
                    throw new RuntimeException("Invalid value for field '" + fieldName + "'. Expected type: "
                            + field.getType().getSimpleName()
                            + (field.getMaxLength() > 0 ? " (max length " + field.getMaxLength() + ")" : ""));
                }
            });
            return new Row(new HashMap<>(data));
        }
    }
}
