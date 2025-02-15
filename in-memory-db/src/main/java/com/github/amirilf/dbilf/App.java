package com.github.amirilf.dbilf;

import com.github.amirilf.dbilf.storage.Database;
import com.github.amirilf.dbilf.storage.Row;
import com.github.amirilf.dbilf.storage.Schema;
import com.github.amirilf.dbilf.storage.Table;

import java.util.HashMap;
import java.util.Map;

public class App {
    public static void main(String[] args) {

        Schema userSchema = new Schema.Builder()
                .addField("id", Long.class, true)
                .addField("username", String.class, false, 20)
                .addField("age", Integer.class, false)
                .build();

        Database db = Database.getDB();
        db.createTable("User", userSchema);

        Table userTable = db.getTable("User");

        // insert
        Map<String, Object> userData = new HashMap<>();
        userData.put("id", 1L);
        userData.put("username", "john_doe");
        userData.put("age", 30);
        Row userRow = new Row(userData);
        userTable.insert(userRow);

        // read
        Row selectedRow = userTable.select(1L);
        System.out.println("Selected Row: " + selectedRow);

        // update
        Map<String, Object> updatedUserData = new HashMap<>();
        updatedUserData.put("id", 1L);
        updatedUserData.put("username", "john_updated");
        updatedUserData.put("age", 31);
        Row updatedRow = new Row(updatedUserData);
        userTable.update(1L, updatedRow);

        // read
        Row updatedSelectedRow = userTable.select(1L);
        System.out.println("Updated Row: " + updatedSelectedRow);

        // delete
        userTable.delete(1L);
        Row deletedRow = userTable.select(1L);
        System.out.println("After deletion, selected row: " + deletedRow); // Should be null.

    }
}
