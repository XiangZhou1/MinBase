package org.minbase.server.table;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionManager {
    private AtomicLong sequenceId = new AtomicLong(0);
    private ConcurrentSkipListMap<Long, Transaction> activeTransactions = new ConcurrentSkipListMap<>();
    private ConcurrentSkipListMap<Long, Transaction> commitedTransactions = new ConcurrentSkipListMap<>();
    private TableManager tableManager;

    public TransactionManager(TableManager tableManager) {
        this.tableManager = tableManager;
    }

    public ConcurrentSkipListMap<Long, Transaction> getActiveTransactions() {
        return activeTransactions;
    }

    public Transaction getActiveTransaction(long txId) {
        return activeTransactions.get(txId);
    }


    public long newTransactionId() {
        return sequenceId.incrementAndGet();
    }

    public Transaction newTransaction() {
        long transactionId = newTransactionId();
        Transaction transaction = new Transaction(transactionId, tableManager);
        activeTransactions.put(transactionId, transaction);
        return transaction;
    }

    public Transaction getTransactionForRecovery(long txId) {
        if (activeTransactions.containsKey(txId)) {
            return activeTransactions.get(txId);
        }
        Transaction transaction = new Transaction(txId, tableManager);
        activeTransactions.put(txId, transaction);
        return transaction;
    }

    public long getCommitId() {
        return sequenceId.incrementAndGet();
    }

    public ConcurrentSkipListMap<Long, Transaction> getCommitedTransactions() {
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

    public void commitTransaction(long txId) {
        Transaction transaction = activeTransactions.remove(txId);
        transaction.setTransactionState(TransactionState.Commit);
        commitedTransactions.put(transaction.getCommitId(), transaction);
        clearCommittedTransaction();
    }

    private void clearCommittedTransaction() {
        if (commitedTransactions.isEmpty()) {
            return;
        }
        if (activeTransactions.isEmpty()) {
            commitedTransactions.clear();
            return;
        }
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

    public boolean validateTransaction(long txId) {
        Transaction transaction = activeTransactions.get(txId);
        Set<byte[]> writeSet = transaction.getWriteSet();
        Set<byte[]> readSet = transaction.getReadSet();
        long committedId = sequenceId.incrementAndGet();
        transaction.setCommittedId(committedId);
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

    private boolean checkConflict(Transaction transaction, Transaction checkedTransaction) {
        Set<byte[]> writeSet = checkedTransaction.getWriteSet();
        for (byte[] bytes : transaction.getReadSet()) {
            if (writeSet.contains(bytes)) {
                return true;
            }
        }
        return false;
    }

    public void rollBackTransaction(long txId) {
        Transaction transaction = activeTransactions.remove(txId);
        transaction.setTransactionState(TransactionState.Rollback);
        clearCommittedTransaction();
    }
}
