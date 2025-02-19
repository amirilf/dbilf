package com.github.amirilf.dbilf;

import com.github.amirilf.dbilf.storage.Database;
import com.github.amirilf.dbilf.storage.Schema;
import com.github.amirilf.dbilf.storage.Row;
import com.github.amirilf.dbilf.storage.Table;

public class App {
    public static void main(String[] args) {
        Database db = Database.getInstance();

        Schema schema = new Schema.Builder()
                .addField("name", String.class, 100)
                .addField("age", Integer.class)
                .build();

        db.createTable("users", schema);
        Table users = db.getTable("users");

        // Insert a new row.
        Row row1 = new Row.Builder(schema)
                .set("name", "John Doe")
                .set("age", 30)
                .build();
        users.create(row1);

        System.out.println(users.getRows().size());

        System.out.println("Before update:");
        users.getRows().forEach(r -> System.out.println(r.getData()));

        // Update the row.
        Long id = (Long) row1.getValue("id");
        Row updatedRow = new Row.Builder(schema)
                .setId(id)
                .set("name", "John Doe")
                .set("age", 35)
                .build();
        users.update(updatedRow);

        System.out.println("After update:");
        users.getRows().forEach(r -> System.out.println(r.getData()));
    }
}
