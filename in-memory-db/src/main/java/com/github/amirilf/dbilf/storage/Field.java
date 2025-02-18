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
        if (!type.isInstance(value)) {
            return false;
        }
        if (type == String.class && maxLength > 0) {
            return ((String) value).length() <= maxLength;
        }
        return true;
    }

    // there could be validation for each type

}
