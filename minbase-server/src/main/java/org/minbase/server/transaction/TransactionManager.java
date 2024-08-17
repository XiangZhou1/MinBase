package org.minbase.server.transaction;


import org.minbase.server.table.Table;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionManager {
    private static AtomicLong sequenceId = new AtomicLong(0);
    private static ConcurrentSkipListMap<Long, Transaction> activeTransactions = new ConcurrentSkipListMap<>();
    private static ConcurrentSkipListMap<Long, Transaction> commitedTransactions = new ConcurrentSkipListMap<>();
    public static Transaction getActiveTransaction(long txId) {
        return activeTransactions.get(txId);
    }
    public static long newTransactionId() {
        return sequenceId.incrementAndGet();
    }

    public static Transaction newTransaction(Map<String, Table> tables) {
        long transactionId = TransactionManager.newTransactionId();
        Transaction transaction = new Transaction(transactionId);
        activeTransactions.put(transactionId, transaction);
        transaction.setTables(tables);
        return transaction;
    }

    public static Transaction newTransaction(Table table) {
        long transactionId = TransactionManager.newTransactionId();
        Transaction transaction = new Transaction(transactionId);
        activeTransactions.put(transactionId, transaction);
        Map<String, Table> tables = new HashMap<>();
        tables.put(table.name(), table);
        transaction.setTables(tables);
        return transaction;
    }

    public static long getCommitId() {
        return sequenceId.incrementAndGet();
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
    });

    public static void commitTransaction(long txId) {
        Transaction transaction = activeTransactions.remove(txId);
        transaction.setTransactionState(TransactionState.Commit);
        commitedTransactions.put(transaction.getCommitId(), transaction);
        clearCommittedTransaction();
    }

    private static void clearCommittedTransaction() {
        long txId = activeTransactions.firstKey();
        Iterator<Map.Entry<Long, Transaction>> iterator = commitedTransactions.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, Transaction> entry = iterator.next();
            long committed = entry.getKey();
            if (committed < txId) {
                iterator.remove();
            }
        }
    }

    public static boolean validateTransaction(long txId) {
        Transaction transaction = activeTransactions.get(txId);
        Set<byte[]> writeSet = transaction.getWriteSet();
        Set<byte[]> readSet = transaction.getReadSet();
        long committedId = sequenceId.incrementAndGet();
        transaction.setCommitId(committedId);
        if (writeSet.isEmpty() || readSet.isEmpty()) {
            return true;
        } else {
            for (Map.Entry<Long, Transaction> entry : commitedTransactions.entrySet()) {
                Long id = entry.getKey();
                Transaction committedTransaction = entry.getValue();
                if (id < committedId && committedTransaction.getTxId() > transaction.getTxId()) {
                    boolean isConflict = checkConflict(transaction, committedTransaction);
                    if (isConflict) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private static boolean checkConflict(Transaction transaction, Transaction checkedTransaction) {
        Set<byte[]> writeSet = checkedTransaction.getWriteSet();
        for (byte[] bytes : transaction.getReadSet()) {
            if (writeSet.contains(bytes)) {
                return true;
            }
        }
        return false;
    }

    public static void rollBackTransaction(long txId) {
        Transaction transaction = activeTransactions.remove(txId);
        transaction.setTransactionState(TransactionState.Rollback);
        clearCommittedTransaction();
    }
}
