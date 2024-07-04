package org.minbase.server.transaction;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionManager {
    public static TransactionLevel transactionLevel = TransactionLevel.READ_COMMIT;
    public static TransactionType transactionType = TransactionType.Optimistic;

    private static AtomicLong transactionId = new AtomicLong(0);
    private static ConcurrentSkipListMap<Long, Transaction> activeTransactions = new ConcurrentSkipListMap<>();


    public static ConcurrentSkipListMap<Long, Transaction> getActiveTransactions() {
        return activeTransactions;
    }

    public static long newTransactionId() {
        return transactionId.incrementAndGet();
    }
}
