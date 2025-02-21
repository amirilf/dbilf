package com.github.amirilf.dbilf.transaction;

import java.util.concurrent.locks.ReentrantLock;

public class TransactionManager {

    private static final ThreadLocal<Transaction> currentTransaction = new ThreadLocal<>();
    private static final ReentrantLock globalLock = new ReentrantLock();

    public static void begin() {
        globalLock.lock();
        Transaction tx = new Transaction();
        currentTransaction.set(tx);
    }

    public static void commit() {
        Transaction tx = currentTransaction.get();
        if (tx == null) {
            throw new RuntimeException("No active transaction");
        }
        tx.clear();
        currentTransaction.remove();
        globalLock.unlock();
    }

    public static void rollback() {
        Transaction tx = currentTransaction.get();
        if (tx == null) {
            throw new RuntimeException("No active transaction");
        }
        tx.rollback();
        currentTransaction.remove();
        globalLock.unlock();
    }

    public static Transaction getCurrentTransaction() {
        return currentTransaction.get();
    }

    public static void register(Operation op) {
        Transaction tx = currentTransaction.get();
        if (tx != null) {
            tx.register(op);
        }
    }
}
