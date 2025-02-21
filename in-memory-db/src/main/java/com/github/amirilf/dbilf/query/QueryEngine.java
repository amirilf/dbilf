package com.github.amirilf.dbilf.query;

import com.github.amirilf.dbilf.storage.Database;
import com.github.amirilf.dbilf.storage.Field;
import com.github.amirilf.dbilf.storage.Table;
import com.github.amirilf.dbilf.storage.Row;
import com.github.amirilf.dbilf.storage.Schema;
import com.github.amirilf.dbilf.transaction.TransactionManager;
import java.util.List;
import java.util.Map;

public class QueryEngine {

    /*
     * BEGIN
     * COMMIT
     * ROLLBACK
     * SHOW TABLES
     * DESCRIBE
     * CREATE TABLE
     * CREATE INDEX
     * INSERT
     * SELECT
     * UPDATE
     * DELETE
     */

    public static String execute(String sql) {

        long startTime = System.nanoTime();
        sql = sql.trim();
        String result;

        // Transaction commands
        if (sql.equalsIgnoreCase("BEGIN")) {
            TransactionManager.begin();
            result = "Transaction started";
        } else if (sql.equalsIgnoreCase("COMMIT")) {
            TransactionManager.commit();
            result = "Transaction committed";
        } else if (sql.equalsIgnoreCase("ROLLBACK")) {
            TransactionManager.rollback();
            result = "Transaction rolled back";
        } else if (sql.equalsIgnoreCase("SHOW TABLES")) {
            result = handleShowTables();
        } else if (sql.toUpperCase().startsWith("DESCRIBE")) {
            result = handleDescribe(sql);
        } else {

            // Other commands
            String[] tokens = sql.split("\\s+");
            String command = tokens[0].toUpperCase();

            if (command.equals("CREATE")) {
                if (tokens.length < 2) {
                    result = "Invalid CREATE syntax";
                } else {
                    String secondToken = tokens[1].toUpperCase();
                    if (secondToken.equals("TABLE")) {
                        // creating table
                        result = handleCreateTable(sql);
                    } else if (secondToken.equals("INDEX")) {
                        // creating index
                        result = handleCreateIndex(sql);
                    } else {
                        result = "Unsupported CREATE command";
                    }
                }
            } else {
                // CRUD in table
                switch (command) {
                    case "INSERT":
                        result = handleInsert(sql);
                        break;
                    case "SELECT":
                        result = handleSelect(sql);
                        break;
                    case "UPDATE":
                        result = handleUpdate(sql);
                        break;
                    case "DELETE":
                        result = handleDelete(sql);
                        break;
                    default:
                        result = "Unsupported command";
                }
            }
        }

        long endTime = System.nanoTime();
        long durationMs = (endTime - startTime) / 1_000_000;
        String detailedTime = String.format("Execution time: %d ms", durationMs);

        return result + "\n" + detailedTime;
    }

    private static String handleShowTables() {
        Database db = Database.getInstance();
        StringBuilder sb = new StringBuilder("Tables:\n");
        for (String tableName : db.getTableNames()) {
            sb.append(tableName).append("\n");
        }
        return sb.toString();
    }

    private static String handleDescribe(String sql) {
        String[] tokens = sql.split("\\s+");
        if (tokens.length != 2) {
            return "Invalid DESCRIBE syntax. Use: DESCRIBE <tableName>";
        }
        String tableName = tokens[1].trim();
        Database db = Database.getInstance();
        Table table = db.getTable(tableName);
        Schema schema = table.getSchema();
        StringBuilder sb = new StringBuilder();
        sb.append("Table: ").append(tableName).append("\nColumns:\n");
        for (Map.Entry<String, Field<?>> entry : schema.getFields().entrySet()) {
            Field<?> field = entry.getValue();
            sb.append(field.getName())
                    .append(" ")
                    .append(field.getType().getSimpleName());
            if (field.getMaxLength() > 0) {
                sb.append(" (max length ").append(field.getMaxLength()).append(")");
            }
            if (field.isPrimaryKey()) {
                sb.append(" PK");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    private static String handleCreateTable(String sql) {
        try {
            String upper = sql.toUpperCase();
            if (!upper.contains("TABLE")) {
                return "Invalid CREATE syntax";
            }
            String[] parts = sql.split("(?i)TABLE");
            String remaining = parts[1].trim();
            String tableName;
            String columnsPart;
            if (remaining.contains("(")) {
                int idxParen = remaining.indexOf("(");
                tableName = remaining.substring(0, idxParen).trim();
                columnsPart = remaining.substring(idxParen + 1, remaining.lastIndexOf(")")).trim();
            } else {
                return "Invalid CREATE TABLE syntax";
            }
            String[] columnDefs = columnsPart.split(",");
            Schema.Builder schemaBuilder = new Schema.Builder();
            for (String colDef : columnDefs) {
                String[] partsCol = colDef.trim().split("\\s+");
                if (partsCol.length < 2) {
                    return "Invalid column definition: " + colDef;
                }
                String colName = partsCol[0].trim();
                String colType = partsCol[1].trim().toUpperCase();
                switch (colType) {
                    case "INTEGER":
                        schemaBuilder.addField(colName, Integer.class);
                        break;
                    case "LONG":
                        schemaBuilder.addField(colName, Long.class);
                        break;
                    case "STRING":
                    case "VARCHAR":
                        if (partsCol.length >= 3) {
                            int length = Integer.parseInt(partsCol[2].trim());
                            schemaBuilder.addField(colName, String.class, length);
                        } else {
                            schemaBuilder.addField(colName, String.class);
                        }
                        break;
                    case "BOOLEAN":
                        schemaBuilder.addField(colName, Boolean.class);
                        break;
                    default:
                        return "Unsupported type: " + colType;
                }
            }
            Schema schema = schemaBuilder.build();
            Database.getInstance().createTable(tableName, schema);
            return "Table " + tableName + " created";
        } catch (Exception e) {
            return "Error in CREATE TABLE: " + e.getMessage();
        }
    }

    private static String handleCreateIndex(String sql) {
        try {
            String upper = sql.toUpperCase();
            if (!upper.contains("ON") || !upper.contains("(") || !upper.contains(")")) {
                return "Invalid CREATE INDEX syntax";
            }

            String remaining = sql.substring("CREATE INDEX".length()).trim();

            boolean unique = false;
            if (remaining.toUpperCase().endsWith("UNIQUE")) {
                unique = true;
                remaining = remaining.substring(0, remaining.length() - "UNIQUE".length()).trim();
            }
            if (!remaining.toUpperCase().startsWith("ON")) {
                return "Invalid CREATE INDEX syntax, missing ON keyword";
            }
            remaining = remaining.substring("ON".length()).trim();

            // remaining parts: <table> (<field>)
            int idxParen = remaining.indexOf('(');
            int endParen = remaining.indexOf(')', idxParen);
            if (idxParen < 0 || endParen < 0) {
                return "Invalid CREATE INDEX syntax, missing parentheses";
            }

            String tableName = remaining.substring(0, idxParen).trim();
            String fieldName = remaining.substring(idxParen + 1, endParen).trim();
            Database db = Database.getInstance();
            Table table = db.getTable(tableName);
            table.addIndex(fieldName, unique);
            return "Index created on " + tableName + " (" + fieldName + ")" + (unique ? " UNIQUE" : "");
        } catch (Exception e) {
            return "Error in CREATE INDEX: " + e.getMessage();
        }
    }

    private static String handleInsert(String sql) {
        try {
            String remaining = sql.substring("INSERT INTO".length()).trim();
            String tableName = remaining.split("\\s+|\\(")[0];
            int startColumns = remaining.indexOf('(');
            int endColumns = remaining.indexOf(')');
            if (startColumns < 0 || endColumns < 0) {
                return "Invalid syntax for INSERT";
            }
            String columnsPart = remaining.substring(startColumns + 1, endColumns).trim();
            String[] columns = columnsPart.split(",");
            int valuesIndex = remaining.toUpperCase().indexOf("VALUES");
            if (valuesIndex < 0) {
                return "VALUES keyword not found";
            }
            int startValues = remaining.indexOf('(', valuesIndex);
            int endValues = remaining.indexOf(')', startValues);
            if (startValues < 0 || endValues < 0) {
                return "Invalid syntax for VALUES";
            }
            String valuesPart = remaining.substring(startValues + 1, endValues).trim();
            String[] values = valuesPart.split(",");
            if (columns.length != values.length) {
                return "Columns count does not match values count";
            }
            Database db = Database.getInstance();
            Table table = db.getTable(tableName);
            Schema schema = table.getSchema();
            Row.Builder builder = new Row.Builder(schema);
            for (int i = 0; i < columns.length; i++) {
                String col = columns[i].trim();
                String val = values[i].trim();
                Object parsedVal = parseValue(val, schema, col);
                builder.set(col, parsedVal);
            }
            Row row = builder.build();
            table.create(row);
            return "Row inserted into " + tableName;
        } catch (Exception e) {
            return "Error in INSERT: " + e.getMessage();
        }
    }

    private static String handleSelect(String sql) {
        try {
            String upper = sql.toUpperCase();
            if (!upper.contains("FROM")) {
                return "Invalid SELECT syntax";
            }
            String[] parts = sql.split("(?i)FROM");
            if (parts.length < 2) {
                return "Invalid SELECT syntax";
            }
            String fromPart = parts[1].trim();
            String tableName;
            String condition = null;
            if (fromPart.contains("WHERE") || fromPart.contains("where")) {
                String[] fromParts = fromPart.split("(?i)WHERE");
                tableName = fromParts[0].trim();
                condition = fromParts[1].trim();
            } else {
                tableName = fromPart.trim();
            }
            Database db = Database.getInstance();
            Table table = db.getTable(tableName);
            List<Row> rows;
            if (condition != null) {
                String[] condParts = condition.split("=");
                if (condParts.length != 2) {
                    return "Invalid WHERE clause";
                }
                String column = condParts[0].trim();
                String valueStr = condParts[1].trim();
                Object value = parseValue(valueStr, table.getSchema(), column);
                rows = table.read(value, column);
            } else {
                rows = table.getRows();
            }
            StringBuilder sb = new StringBuilder();
            for (Row row : rows) {
                sb.append(row.getData().toString()).append("\n");
            }
            return sb.toString();
        } catch (Exception e) {
            return "Error in SELECT: " + e.getMessage();
        }
    }

    private static String handleUpdate(String sql) {
        try {
            String upper = sql.toUpperCase();
            if (!upper.contains("SET")) {
                return "Invalid UPDATE syntax";
            }
            String[] parts = sql.split("(?i)SET");
            String tableNamePart = parts[0].replaceFirst("(?i)UPDATE", "").trim();
            String setPart;
            String wherePart = null;
            if (parts[1].toUpperCase().contains("WHERE")) {
                String[] setWhere = parts[1].split("(?i)WHERE");
                setPart = setWhere[0].trim();
                wherePart = setWhere[1].trim();
            } else {
                setPart = parts[1].trim();
            }
            Database db = Database.getInstance();
            Table table = db.getTable(tableNamePart);
            if (wherePart == null) {
                return "UPDATE must include WHERE clause on primary key";
            }
            String[] condParts = wherePart.split("=");
            if (condParts.length != 2) {
                return "Invalid WHERE clause in UPDATE";
            }
            String col = condParts[0].trim();
            String valStr = condParts[1].trim();
            if (!col.equalsIgnoreCase("id")) {
                return "UPDATE supported only on primary key (id)";
            }
            Object id = parseValue(valStr, table.getSchema(), "id");
            List<Row> existingRows = table.read(id, "id");
            if (existingRows.size() != 1) {
                return "Row not found for id: " + id;
            }
            Row oldRow = existingRows.get(0);
            Row.Builder builder = new Row.Builder(table.getSchema());
            oldRow.getData().forEach((k, v) -> {
                if (!k.equals("id")) {
                    builder.set(k, v);
                } else {
                    builder.setId(v);
                }
            });
            String[] assignments = setPart.split(",");
            for (String assign : assignments) {
                String[] pair = assign.split("=");
                if (pair.length != 2) {
                    return "Invalid SET clause";
                }
                String column = pair[0].trim();
                String valueStr = pair[1].trim();
                Object value = parseValue(valueStr, table.getSchema(), column);
                if (column.equalsIgnoreCase("id")) {
                    continue;
                }
                builder.set(column, value);
            }
            Row newRow = builder.build();
            table.update(newRow);
            return "Row updated in " + tableNamePart;
        } catch (Exception e) {
            return "Error in UPDATE: " + e.getMessage();
        }
    }

    private static String handleDelete(String sql) {
        try {
            String upper = sql.toUpperCase();
            if (!upper.contains("FROM") || !upper.contains("WHERE")) {
                return "Invalid DELETE syntax";
            }
            String[] parts = sql.split("(?i)FROM");
            String afterFrom = parts[1].trim();
            String[] tableAndWhere = afterFrom.split("(?i)WHERE");
            String tableName = tableAndWhere[0].trim();
            String condition = tableAndWhere[1].trim();
            String[] condParts = condition.split("=");
            if (condParts.length != 2) {
                return "Invalid WHERE clause in DELETE";
            }
            String col = condParts[0].trim();
            String valStr = condParts[1].trim();
            if (!col.equalsIgnoreCase("id")) {
                return "DELETE supported only on primary key (id)";
            }
            Object id = parseValue(valStr, Database.getInstance().getTable(tableName).getSchema(), "id");
            Database.getInstance().getTable(tableName).delete((Long) id);
            return "Row deleted from " + tableName;
        } catch (Exception e) {
            return "Error in DELETE: " + e.getMessage();
        }
    }

    private static Object parseValue(String val, Schema schema, String column) {
        if (val.startsWith("'") && val.endsWith("'")) {
            return val.substring(1, val.length() - 1);
        }
        // for the pk field "id", always parse as Long
        if (column.equalsIgnoreCase("id")) {
            try {
                return Long.parseLong(val);
            } catch (NumberFormatException e) {
                throw new RuntimeException("Invalid numeric format for field '" + column + "'. Expected Long");
            }
        }
        // parsing based on the expected type from the schema
        Class<?> expectedType = schema.getFields().get(column).getType();
        if (expectedType.equals(Integer.class)) {
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
        }
        return val;
    }

}
