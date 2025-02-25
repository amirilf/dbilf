package com.github.amirilf.dbilf.transaction;

public class TransactionManager {
    private static final ThreadLocal<Transaction> currentTransaction = new ThreadLocal<>();

    public static void begin() {
        if (currentTransaction.get() != null) {
            throw new IllegalStateException("Transaction already active");
        }
        currentTransaction.set(new Transaction());
    }

    public static void commit() {
        Transaction tx = currentTransaction.get();
        if (tx == null) {
            throw new IllegalStateException("No active transaction");
        }
        try {
            tx.commit();
        } finally {
            currentTransaction.remove();
        }
    }

    public static void rollback() {
        Transaction tx = currentTransaction.get();
        if (tx == null) {
            throw new IllegalStateException("No active transaction");
        }
        try {
            tx.rollback();
        } finally {
            currentTransaction.remove();
        }
    }

    public static Transaction getCurrentTransaction() {
        return currentTransaction.get();
    }

    public static void register(Operation op) {
        Transaction tx = currentTransaction.get();
        if (tx != null && tx.isActive()) {
            tx.register(op);
        }
    }
}