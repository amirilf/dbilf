package com.github.amirilf.dbilf.storage;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@Getter
@AllArgsConstructor
@ToString
public class Field<T> {

    private final String name;
    private final Class<T> type;
    private final boolean primaryKey;
    private final int maxLength; // for String/VARCHAR types

    // check the given value against this field's type.
    public boolean validate(Object value) {

        if (value == null)
            return false;

        if (!type.isInstance(value))
            return false;

        // for strings (varchars must satisfy the length limit)
        if (type == String.class && maxLength > 0) {
            return ((String) value).length() <= maxLength;
        }

        return true;
    }

}
