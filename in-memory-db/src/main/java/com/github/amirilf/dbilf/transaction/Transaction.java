package com.github.amirilf.dbilf.transaction;

import java.util.ArrayList;
import java.util.List;

public class Transaction {

    private final List<Operation> undos = new ArrayList<>();

    public void register(Operation op) {
        undos.add(op);
    }

    public void rollback() {
        for (int i = undos.size() - 1; i >= 0; i--) {
            try {
                undos.get(i).undo();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        undos.clear();
    }

    public void clear() {
        undos.clear();
    }
}
