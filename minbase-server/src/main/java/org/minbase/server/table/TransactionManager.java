package org.minbase.server.table;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    private ReentrantReadWriteLock transactionUpdateLock = new ReentrantReadWriteLock();


    public void setSequenceId(long id) {
        sequenceId.set(Math.max(sequenceId.get(), id));
    }

    public long newTransactionId() {
        return sequenceId.incrementAndGet();
    }

    public Transaction newTransaction() {
        transactionWriteLock();
        try {
            long transactionId = newTransactionId();
            Transaction transaction = new Transaction(transactionId, tableManager);
            activeTransactions.put(transactionId, transaction);
            return transaction;
        } finally {
            transactionWriteUnLock();
        }
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
        transactionWriteLock();
        try {
            Transaction transaction = activeTransactions.remove(txId);
            transaction.setTransactionState(TransactionState.Commit);
            commitedTransactions.put(transaction.getCommitId(), transaction);
            clearCommittedTransaction();
        } finally {
            transactionWriteUnLock();
        }
    }

    private void clearCommittedTransaction() {
        transactionWriteLock();
        try {
            if (commitedTransactions.isEmpty()) {
                return;
            }
            if (activeTransactions.isEmpty()) {
                //System.out.println("Clear all commit transaction, firstActiveTransaction" + firstActiveTransaction + ", cleard:" + );
                commitedTransactions.clear();
                return;
            }
            long txId = activeTransactions.firstKey();
            Iterator<Map.Entry<Long, Transaction>> iterator = commitedTransactions.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, Transaction> entry = iterator.next();
                long committed = entry.getKey();
                if (committed < txId) {
                    System.out.println("Clear commit transaction, active firstTxid" + txId + ", cleared transaction:" + entry.getValue());
                    iterator.remove();
                }
            }
        } finally {
            transactionWriteUnLock();
        }

    }

    public boolean validateTransaction(long txId) {
        transactionReadLock();
        try {
            Transaction transaction = activeTransactions.get(txId);
            KeySet readSet = transaction.getReadSet();
            long committedId = sequenceId.incrementAndGet();
            transaction.setCommittedId(committedId);
            if (readSet.isEmpty()) {
                return true;
            } else {
                for (Map.Entry<Long, Transaction> otherEntry : commitedTransactions.entrySet()) {
                    Long otherCommitId = otherEntry.getKey();
                    Transaction otherCommittedTransaction = otherEntry.getValue();
                    if (transaction.getTxId() < otherCommitId && transaction.getCommitId() > otherCommitId) {
                        transaction.addCheckTransaction(otherCommittedTransaction);
                        boolean isConflict = checkConflict(transaction, otherCommittedTransaction);
                        if (isConflict) {
                            return false;
                        }
                    }
                }
            }
            return true;
        } finally {
            transactionReadUnLock();
        }
    }

    private boolean checkConflict(Transaction transaction, Transaction checkedTransaction) {
        KeySet writeSet = checkedTransaction.getWriteSet();
        KeySet readSet = transaction.getReadSet();
        return writeSet.isOverLap(readSet);
    }

    public void rollBackTransaction(long txId) {
        transactionWriteLock();
        try {
            Transaction transaction = activeTransactions.remove(txId);
            transaction.setTransactionState(TransactionState.Rollback);
            clearCommittedTransaction();
        } finally {
            transactionWriteUnLock();
        }
    }

    public void transactionReadLock() {
        transactionUpdateLock.readLock().lock();
    }
    public void transactionReadUnLock() {
        transactionUpdateLock.readLock().unlock();
    }
    public void transactionWriteLock() {
        transactionUpdateLock.writeLock().lock();
    }
    public void transactionWriteUnLock() {
        transactionUpdateLock.writeLock().unlock();
    }

    public long getTransactionMinReadPoint() {
        transactionReadLock();
        long minReadPoint = Long.MAX_VALUE;
        try {
            for (Transaction transaction : getActiveTransactions().values()) {
                minReadPoint = Math.min(transaction.getReadPoint(), minReadPoint);
            }
            return minReadPoint;
        } finally {
            transactionReadUnLock();
        }
    }
}
