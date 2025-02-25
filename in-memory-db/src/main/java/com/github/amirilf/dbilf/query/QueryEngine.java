package com.github.amirilf.dbilf.query;

import com.github.amirilf.dbilf.storage.Database;
import com.github.amirilf.dbilf.storage.Row;
import com.github.amirilf.dbilf.storage.Schema;
import com.github.amirilf.dbilf.storage.Table;
import com.github.amirilf.dbilf.transaction.TransactionManager;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class QueryEngine {

    public static String execute(String sql) {
        long startTime = System.nanoTime();
        String result;
        try {
            Command cmd = SQLParser.parse(sql);
            result = executeCommand(cmd);
        } catch (Exception e) {
            result = "Error: " + e.getMessage();
        }
        long endTime = System.nanoTime();
        long durationMs = (endTime - startTime) / 1_000_000;
        return result + "\nExecution time: " + durationMs + " ms";
    }

    private static String executeCommand(Command cmd) {
        switch (cmd.getType()) {
            case BEGIN:
                TransactionManager.begin();
                return "Transaction started";
            case COMMIT:
                TransactionManager.commit();
                return "Transaction committed";
            case ROLLBACK:
                TransactionManager.rollback();
                return "Transaction rolled back";
            case SHOW_TABLES:
                return handleShowTables();
            case CREATE_TABLE:
                return handleCreateTable(cmd);
            case DELETE_TABLE:
                return handleDeleteTable(cmd);
            case CREATE_INDEX:
                return handleCreateIndex(cmd);
            case REMOVE_INDEX:
                return handleRemoveIndex(cmd);
            case INSERT:
                return handleInsert(cmd);
            case SELECT:
                return handleSelect(cmd);
            case UPDATE:
                return handleUpdate(cmd);
            case DELETE:
                return handleDelete(cmd);
            default:
                throw new RuntimeException("Unsupported command type");
        }
    }

    private static String handleShowTables() {
        Database db = Database.getInstance();
        StringBuilder sb = new StringBuilder("Tables:\n");
        for (String tableName : db.getTableNames()) {
            sb.append(tableName).append("\n");
        }
        return sb.toString();
    }

    private static String handleCreateTable(Command cmd) {
        try {
            Schema.Builder schemaBuilder = new Schema.Builder();
            Map<String, Object> colDefs = cmd.getColumnDefinitions();
            for (Map.Entry<String, Object> entry : colDefs.entrySet()) {
                String colName = entry.getKey();
                Object[] typeInfo = (Object[]) entry.getValue();
                Class<?> type = (Class<?>) typeInfo[0];
                int maxLength = (Integer) typeInfo[1];
                if (type.equals(char[].class)) {
                    schemaBuilder.addField(colName, char[].class, maxLength);
                } else {
                    schemaBuilder.addField(colName, type, maxLength);
                }
            }
            Schema schema = schemaBuilder.build();
            Database.getInstance().createTable(cmd.getTableName(), schema);
            return "Table " + cmd.getTableName() + " created";
        } catch (Exception e) {
            return "Error in CREATE TABLE: " + e.getMessage();
        }
    }

    private static String handleDeleteTable(Command cmd) {
        try {
            Database.getInstance().deleteTable(cmd.getTableName());
            return "Table " + cmd.getTableName() + " deleted";
        } catch (Exception e) {
            return "Error in DELETE TABLE: " + e.getMessage();
        }
    }

    private static String handleCreateIndex(Command cmd) {
        try {
            Table table = Database.getInstance().getTable(cmd.getTableName());
            table.addIndex(cmd.getIndexField(), cmd.isUniqueIndex());
            return "Index created on " + cmd.getTableName() + " (" + cmd.getIndexField() + ")"
                    + (cmd.isUniqueIndex() ? " UNIQUE" : "");
        } catch (Exception e) {
            return "Error in CREATE INDEX: " + e.getMessage();
        }
    }

    private static String handleRemoveIndex(Command cmd) {
        try {
            Table table = Database.getInstance().getTable(cmd.getTableName());
            table.removeIndex(cmd.getIndexField());
            return "Index removed from " + cmd.getTableName() + " (" + cmd.getIndexField() + ")";
        } catch (Exception e) {
            return "Error in REMOVE INDEX: " + e.getMessage();
        }
    }

    private static String handleInsert(Command cmd) {
        try {
            Table table = Database.getInstance().getTable(cmd.getTableName());
            Schema schema = table.getSchema();
            Row.Builder builder = new Row.Builder(schema);
            Map<String, Object> values = cmd.getInsertValues();
            for (Map.Entry<String, Object> entry : values.entrySet()) {
                String col = entry.getKey();
                Object val = parseValue(entry.getValue().toString(), schema, col);
                builder.set(col, val);
            }
            Row row = builder.build();
            table.create(row);
            return "Row inserted into " + cmd.getTableName();
        } catch (Exception e) {
            return "Error in INSERT: " + e.getMessage();
        }
    }

    private static String handleSelect(Command cmd) {
        try {
            Table table = Database.getInstance().getTable(cmd.getTableName());
            Schema schema = table.getSchema();
            // Validate requested columns (if not "*")
            if (cmd.getSelectColumns() != null && !cmd.getSelectColumns().isEmpty()) {
                for (String col : cmd.getSelectColumns()) {
                    if (!schema.getFields().containsKey(col)) {
                        throw new RuntimeException("Column not found: " + col);
                    }
                }
            }
            List<Row> rows;
            if (cmd.getConditionColumn() != null) {
                Object value = parseValue(cmd.getConditionValue().toString(), schema, cmd.getConditionColumn());
                rows = table.read(value, cmd.getConditionColumn());
            } else {
                rows = table.getRows();
            }
            StringBuilder sb = new StringBuilder();
            for (Row row : rows) {
                if (cmd.getSelectColumns() == null || cmd.getSelectColumns().isEmpty()) {
                    // For '*' selection, iterate over each key-value pair using formatting
                    StringJoiner joiner = new StringJoiner(", ");
                    for (Map.Entry<String, Object> entry : row.getData().entrySet()) {
                        joiner.add(entry.getKey() + "=" + formatValue(entry.getValue()));
                    }
                    sb.append(joiner.toString()).append("\n");
                } else {
                    StringJoiner joiner = new StringJoiner(", ");
                    for (String col : cmd.getSelectColumns()) {
                        Object val = row.getData().get(col);
                        joiner.add(col + "=" + formatValue(val));
                    }
                    sb.append(joiner.toString()).append("\n");
                }
            }
            return sb.toString();
        } catch (Exception e) {
            return "Error in SELECT: " + e.getMessage();
        }
    }

    private static String handleUpdate(Command cmd) {
        try {
            Table table = Database.getInstance().getTable(cmd.getTableName());
            if (cmd.getConditionColumn() == null)
                return "UPDATE must include WHERE clause on primary key";
            Object id = parseValue(cmd.getConditionValue().toString(), table.getSchema(), cmd.getConditionColumn());
            List<Row> rows = table.read(id, cmd.getConditionColumn());
            if (rows.size() != 1)
                return "Row not found for id: " + id;
            Row oldRow = rows.get(0);
            Row.Builder builder = new Row.Builder(table.getSchema());
            oldRow.getData().forEach((k, v) -> {
                if (!k.equals("id")) {
                    builder.set(k, v);
                } else {
                    builder.setId(v);
                }
            });
            for (Map.Entry<String, Object> entry : cmd.getUpdateValues().entrySet()) {
                if (entry.getKey().equalsIgnoreCase("id"))
                    continue;
                Object value = parseValue(entry.getValue().toString(), table.getSchema(), entry.getKey());
                builder.set(entry.getKey(), value);
            }
            Row newRow = builder.build();
            table.update(newRow);
            return "Row updated in " + cmd.getTableName();
        } catch (Exception e) {
            return "Error in UPDATE: " + e.getMessage();
        }
    }

    private static String handleDelete(Command cmd) {
        try {
            Table table = Database.getInstance().getTable(cmd.getTableName());
            if (cmd.getConditionColumn() == null)
                return "DELETE must include WHERE clause on primary key";
            Object id = parseValue(cmd.getConditionValue().toString(), table.getSchema(), cmd.getConditionColumn());
            table.delete((Long) id);
            return "Row deleted from " + cmd.getTableName();
        } catch (Exception e) {
            return "Error in DELETE: " + e.getMessage();
        }
    }

    private static Object parseValue(String val, Schema schema, String column) {
        Class<?> expectedType = schema.getFields().get(column).getType();
        if (expectedType.equals(char[].class)) {
            return val.toCharArray();
        } else if (expectedType.equals(Integer.class)) {
            try {
                return Integer.parseInt(val);
            } catch (NumberFormatException e) {
                throw new RuntimeException("Invalid value for field '" + column + "'. Expected type: Integer");
            }
        } else if (expectedType.equals(Long.class)) {
            try {
                return Long.parseLong(val);
            } catch (NumberFormatException e) {
                throw new RuntimeException("Invalid value for field '" + column + "'. Expected type: Long");
            }
        } else if (expectedType.equals(Boolean.class)) {
            return Boolean.parseBoolean(val);
        }
        return val;
    }

    private static String formatValue(Object value) {
        if (value instanceof char[]) {
            return new String((char[]) value);
        }
        return String.valueOf(value);
    }
}
