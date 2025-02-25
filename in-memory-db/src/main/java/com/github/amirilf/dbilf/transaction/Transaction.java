package com.github.amirilf.dbilf.transaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Transaction {
    private final List<Operation> undos = new ArrayList<>();
    private final List<Runnable> lockReleases = new ArrayList<>();
    private boolean active = true;

    public void register(Operation op) {
        if (active)
            undos.add(op);
    }

    public void registerLockRelease(Runnable release) {
        if (active)
            lockReleases.add(release);
    }

    public void commit() {
        if (!active)
            return;
        active = false;
        undos.clear();
        releaseLocks();
    }

    public void rollback() {
        if (!active)
            return;
        active = false;
        Collections.reverse(undos);
        undos.forEach(op -> {
            try {
                op.undo();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        undos.clear();
        releaseLocks();
    }

    private void releaseLocks() {
        lockReleases.forEach(Runnable::run);
        lockReleases.clear();
    }

    public boolean isActive() {
        return active;
    }
}