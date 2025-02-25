package com.github.amirilf.dbilf.query;

import java.util.*;

public class SQLParser {
    public static Command parse(String sql) {
        String trimmed = sql.trim();
        String upper = trimmed.toUpperCase();
        Command command = new Command();

        if (upper.startsWith("BEGIN")) {
            command.setType(CommandType.BEGIN);
        } else if (upper.startsWith("COMMIT")) {
            command.setType(CommandType.COMMIT);
        } else if (upper.startsWith("ROLLBACK")) {
            command.setType(CommandType.ROLLBACK);
        } else if (upper.startsWith("SHOW TABLES")) {
            command.setType(CommandType.SHOW_TABLES);
        } else if (upper.startsWith("DESCRIBE")) {
            command.setType(CommandType.SHOW_TABLES);
            String[] parts = trimmed.split("\\s+");
            if (parts.length > 1) {
                command.setTableName(parts[1]);
            }
        } else if (upper.startsWith("CREATE TABLE")) {
            command.setType(CommandType.CREATE_TABLE);
            int idxTable = upper.indexOf("TABLE");
            String rest = trimmed.substring(idxTable + "TABLE".length()).trim();
            int idxParen = rest.indexOf("(");
            if (idxParen < 0)
                throw new RuntimeException("Invalid CREATE TABLE syntax");
            String tableName = rest.substring(0, idxParen).trim();
            command.setTableName(tableName);
            int idxEnd = rest.lastIndexOf(")");
            String cols = rest.substring(idxParen + 1, idxEnd).trim();
            String[] colDefs = cols.split(",");
            Map<String, Object> colDefsMap = new LinkedHashMap<>();
            // Each column: name type [maxLength]
            for (String colDef : colDefs) {
                String[] parts = colDef.trim().split("\\s+");
                if (parts.length < 2)
                    throw new RuntimeException("Invalid column definition: " + colDef);
                String colName = parts[0].trim();
                String colType = parts[1].trim().toUpperCase();
                Integer maxLength = null;
                if (parts.length >= 3) {
                    try {
                        maxLength = Integer.parseInt(parts[2].trim());
                    } catch (Exception e) {
                        // ignore if not a number
                    }
                }
                if (colType.equals("VARCHAR") || colType.equals("STRING")) {
                    colDefsMap.put(colName, new Object[] { char[].class, (maxLength != null ? maxLength : 0) });
                } else if (colType.equals("INTEGER")) {
                    colDefsMap.put(colName, new Object[] { Integer.class, 0 });
                } else if (colType.equals("LONG")) {
                    colDefsMap.put(colName, new Object[] { Long.class, 0 });
                } else if (colType.equals("BOOLEAN")) {
                    colDefsMap.put(colName, new Object[] { Boolean.class, 0 });
                } else {
                    throw new RuntimeException("Unsupported type: " + colType);
                }
            }
            command.setColumnDefinitions(colDefsMap);
        } else if (upper.startsWith("DROP TABLE")) {
            command.setType(CommandType.DELETE_TABLE);
            String[] parts = trimmed.split("\\s+");
            if (parts.length < 3)
                throw new RuntimeException("Invalid DROP TABLE syntax");
            command.setTableName(parts[2]);
        } else if (upper.startsWith("CREATE INDEX")) {
            command.setType(CommandType.CREATE_INDEX);
            String rest = trimmed.substring("CREATE INDEX".length()).trim();
            boolean unique = false;
            if (rest.toUpperCase().endsWith("UNIQUE")) {
                unique = true;
                rest = rest.substring(0, rest.length() - "UNIQUE".length()).trim();
            }
            if (!rest.toUpperCase().startsWith("ON")) {
                throw new RuntimeException("Invalid CREATE INDEX syntax");
            }
            rest = rest.substring("ON".length()).trim();
            int idxParen = rest.indexOf("(");
            int idxEnd = rest.indexOf(")", idxParen);
            if (idxParen < 0 || idxEnd < 0)
                throw new RuntimeException("Invalid CREATE INDEX syntax");
            String tableName = rest.substring(0, idxParen).trim();
            String field = rest.substring(idxParen + 1, idxEnd).trim();
            command.setTableName(tableName);
            command.setIndexField(field);
            command.setUniqueIndex(unique);
        } else if (upper.startsWith("DROP INDEX")) {
            command.setType(CommandType.REMOVE_INDEX);
            String rest = trimmed.substring("DROP INDEX".length()).trim();
            if (!rest.toUpperCase().startsWith("ON")) {
                throw new RuntimeException("Invalid DROP INDEX syntax");
            }
            rest = rest.substring("ON".length()).trim();
            int idxParen = rest.indexOf("(");
            int idxEnd = rest.indexOf(")", idxParen);
            if (idxParen < 0 || idxEnd < 0)
                throw new RuntimeException("Invalid DROP INDEX syntax");
            String tableName = rest.substring(0, idxParen).trim();
            String field = rest.substring(idxParen + 1, idxEnd).trim();
            command.setTableName(tableName);
            command.setIndexField(field);
        } else if (upper.startsWith("INSERT INTO")) {
            command.setType(CommandType.INSERT);
            String rest = trimmed.substring("INSERT INTO".length()).trim();
            int idxParen = rest.indexOf("(");
            String tableName = rest.substring(0, idxParen).trim();
            command.setTableName(tableName);
            int idxEndCols = rest.indexOf(")");
            String colsPart = rest.substring(idxParen + 1, idxEndCols).trim();
            String[] columns = colsPart.split(",");
            int idxValues = rest.toUpperCase().indexOf("VALUES");
            int idxParenValues = rest.indexOf("(", idxValues);
            int idxEndValues = rest.indexOf(")", idxParenValues);
            String valsPart = rest.substring(idxParenValues + 1, idxEndValues).trim();
            String[] values = valsPart.split(",");
            if (columns.length != values.length)
                throw new RuntimeException("Columns count does not match values count");
            Map<String, Object> insertMap = new LinkedHashMap<>();
            for (int i = 0; i < columns.length; i++) {
                String col = columns[i].trim();
                String val = values[i].trim();
                if (val.startsWith("'") && val.endsWith("'")) {
                    val = val.substring(1, val.length() - 1);
                }
                insertMap.put(col, val);
            }
            command.setInsertValues(insertMap);
        } else if (upper.startsWith("SELECT")) {
            command.setType(CommandType.SELECT);
            int idxFrom = upper.indexOf("FROM");
            String colsPart = trimmed.substring("SELECT".length(), idxFrom).trim();
            List<String> selectColumns = new ArrayList<>();
            if (!colsPart.equals("*")) {
                String[] cols = colsPart.split(",");
                for (String c : cols) {
                    selectColumns.add(c.trim());
                }
            }
            command.setSelectColumns(selectColumns);
            String rest = trimmed.substring(idxFrom + "FROM".length()).trim();
            String tableName;
            String conditionColumn = null;
            Object conditionValue = null;
            if (rest.toUpperCase().contains("WHERE")) {
                String[] parts = rest.split("(?i)WHERE");
                tableName = parts[0].trim();
                String cond = parts[1].trim();
                String[] condParts = cond.split("=");
                if (condParts.length != 2)
                    throw new RuntimeException("Invalid WHERE clause");
                conditionColumn = condParts[0].trim();
                String condVal = condParts[1].trim();
                if (condVal.startsWith("'") && condVal.endsWith("'")) {
                    condVal = condVal.substring(1, condVal.length() - 1);
                }
                conditionValue = condVal;
            } else {
                tableName = rest.trim();
            }
            command.setTableName(tableName);
            command.setConditionColumn(conditionColumn);
            command.setConditionValue(conditionValue);
        } else if (upper.startsWith("UPDATE")) {
            command.setType(CommandType.UPDATE);
            int idxSet = upper.indexOf("SET");
            String tableName = trimmed.substring("UPDATE".length(), idxSet).trim();
            command.setTableName(tableName);
            String rest = trimmed.substring(idxSet + "SET".length()).trim();
            String setPart;
            String wherePart = null;
            if (rest.toUpperCase().contains("WHERE")) {
                String[] parts = rest.split("(?i)WHERE");
                setPart = parts[0].trim();
                wherePart = parts[1].trim();
            } else {
                setPart = rest;
            }
            Map<String, Object> updateMap = new LinkedHashMap<>();
            String[] assignments = setPart.split(",");
            for (String assign : assignments) {
                String[] pair = assign.split("=");
                if (pair.length != 2)
                    throw new RuntimeException("Invalid SET clause");
                String col = pair[0].trim();
                String val = pair[1].trim();
                if (val.startsWith("'") && val.endsWith("'")) {
                    val = val.substring(1, val.length() - 1);
                }
                updateMap.put(col, val);
            }
            command.setUpdateValues(updateMap);
            if (wherePart != null) {
                String[] condParts = wherePart.split("=");
                if (condParts.length != 2)
                    throw new RuntimeException("Invalid WHERE clause in UPDATE");
                String condCol = condParts[0].trim();
                String condVal = condParts[1].trim();
                if (condVal.startsWith("'") && condVal.endsWith("'")) {
                    condVal = condVal.substring(1, condVal.length() - 1);
                }
                command.setConditionColumn(condCol);
                command.setConditionValue(condVal);
            }
        } else if (upper.startsWith("DELETE")) {
            command.setType(CommandType.DELETE);
            int idxFrom = upper.indexOf("FROM");
            String rest = trimmed.substring(idxFrom + "FROM".length()).trim();
            String tableName;
            String wherePart = null;
            if (rest.toUpperCase().contains("WHERE")) {
                String[] parts = rest.split("(?i)WHERE");
                tableName = parts[0].trim();
                wherePart = parts[1].trim();
            } else {
                tableName = rest.trim();
            }
            command.setTableName(tableName);
            if (wherePart != null) {
                String[] condParts = wherePart.split("=");
                if (condParts.length != 2)
                    throw new RuntimeException("Invalid WHERE clause in DELETE");
                String condCol = condParts[0].trim();
                String condVal = condParts[1].trim();
                if (condVal.startsWith("'") && condVal.endsWith("'")) {
                    condVal = condVal.substring(1, condVal.length() - 1);
                }
                command.setConditionColumn(condCol);
                command.setConditionValue(condVal);
            }
        } else {
            throw new RuntimeException("Unsupported command");
        }
        return command;
    }
}
