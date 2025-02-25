package com.github.amirilf.dbilf.storage;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public final class Field<T> {

    private final String name;
    private final Class<T> type;
    private final boolean primaryKey;
    private final int maxLength;

    public boolean validate(Object value) {
        if (value == null) {
            return false;
        }
        if (type.equals(char[].class)) {
            if (!(value instanceof char[])) {
                return false;
            }
            if (maxLength > 0 && ((char[]) value).length > maxLength) {
                return false;
            }
            return true;
        }
        if (!type.isInstance(value)) {
            return false;
        }
        if (type == String.class && maxLength > 0) {
            return ((String) value).length() <= maxLength;
        }
        return true;
    }
}
