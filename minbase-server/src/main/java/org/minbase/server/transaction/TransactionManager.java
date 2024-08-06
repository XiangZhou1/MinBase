package org.minbase.server.transaction;

import org.minbase.server.constant.Constants;
import org.minbase.server.minstore.MinStore;
import org.minbase.server.table.TableImpl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionManager {
    public static TransactionType transactionType = TransactionType.Pessimistic;

    private static AtomicLong transactionId = new AtomicLong(0);
    private static ConcurrentSkipListMap<Long, Transaction> activeTransactions = new ConcurrentSkipListMap<>();


    public static ConcurrentSkipListMap<Long, Transaction> getActiveTransactions() {
        return activeTransactions;
    }

    public static long newTransactionId() {
        return transactionId.incrementAndGet();
    }

    public static Transaction newTransaction(Map<String, TableImpl> tables) {
        long snapShot = Constants.LATEST_VERSION;
        long transactionId = TransactionManager.newTransactionId();
        Transaction transaction;
        if (TransactionManager.transactionType.equals(TransactionType.Optimistic)) {
            transaction = new OptimisticTransaction(transactionId);
        } else {
            transaction = new PessimisticTransaction(transactionId);
        }
        activeTransactions.put(transactionId, transaction);
        transaction.setTables(tables);
        transaction.setSnapShot(snapShot);
        return transaction;
    }
}
