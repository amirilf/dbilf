package com.github.amirilf.dbilf.index;

import com.github.amirilf.dbilf.storage.Row;
import java.util.List;

public interface Index {
    void insert(Row row);

    void update(Row oldRow, Row newRow);

    void delete(Row row);

    List<Row> search(Object key);

    String getFieldName();

    boolean isUnique();
}
