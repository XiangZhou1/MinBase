package org.minbase.server.transaction;

import org.minbase.server.table.TableImpl;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionManager {
    private static AtomicLong txId = new AtomicLong(0);
    private static ConcurrentSkipListMap<Long, Transaction> activeTransactions = new ConcurrentSkipListMap<>();
    private static ConcurrentSkipListMap<Long, Transaction> commitedTransactions = new ConcurrentSkipListMap<>();


    public static ConcurrentSkipListMap<Long, Transaction> getActiveTransactions() {
        return activeTransactions;
    }

    public static long newTransactionId() {
        return txId.incrementAndGet();
    }

    public static Transaction newTransaction(Map<String, TableImpl> tables) {
        long transactionId = TransactionManager.newTransactionId();
        Transaction transaction = new Transaction(transactionId);
        activeTransactions.put(transactionId, transaction);
        transaction.setTables(tables);
        return transaction;
    }

    public static long getCommitId() {
        return txId.incrementAndGet();
    }

    public static ConcurrentSkipListMap<Long, Transaction> getCommitedTransactions() {
        return commitedTransactions;
    }


    Thread thread = new Thread(new Runnable() {
        @Override
        public void run() {
            Map.Entry<Long, Transaction> e = commitedTransactions.firstEntry();
            if (e != null) {
                Transaction transaction = e.getValue();

            }
        }
    })
}
