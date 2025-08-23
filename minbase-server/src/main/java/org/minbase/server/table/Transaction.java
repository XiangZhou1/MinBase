package org.minbase.server.table;



import org.minbase.common.exception.TransactionException;
import org.minbase.common.table.Table;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.WriteBatch;
import org.minbase.server.kv.store.StoreManager;
import org.minbase.server.table.wal.Wal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Transaction implements org.minbase.common.table.transaction.Transaction {
    private static final Logger LOG = LoggerFactory.getLogger(Transaction.class);
    protected long txId;
    private long commitId;
    protected TransactionState transactionState;
    protected StoreManager storeManager;
    protected TransactionManager transactionManager;
    protected KeySet writeSet;
    protected KeySet readSet;
    private Map<String, TransactionTable> txTables = new HashMap<>();
    private WriteBatch writeBatch;
    private Wal wal;
    private long readPoint = Long.MAX_VALUE;

    public Transaction(long transactionId, TableManager tableManager) {
        this.txId = transactionId;
        this.transactionState = TransactionState.Active;
        this.writeBatch = new WriteBatch();
        this.writeSet = new KeySet();
        this.readSet = new KeySet();
        this.storeManager = tableManager.getStoreManager();
        this.transactionManager = tableManager.getTransactionManager();
        this.wal = tableManager.getWal();
        this.readPoint = storeManager.getReadPoint();
    }

    @Override
    public long txId() {
        return txId;
    }

    @Override
    public Table getTable(String tableName) {
        TransactionTable transactionTable = txTables.get(tableName);
        if (transactionTable == null) {
            transactionTable = new TransactionTable(tableName, this);
            txTables.put(tableName, transactionTable);
        }
        return transactionTable;
    }

    public long getTxId() {
        return txId;
    }

    public TransactionState getTransactionState() {
        return transactionState;
    }


    public void commit() throws TransactionException {
        transactionManager.transactionWriteLock();
        try {
            if (!transactionManager.validateTransaction(txId)) {
                throw new TransactionException("validate fail");
            }

            if (!writeBatch.isEmpty()) {
                synchronized (Transaction.class) {
                    writeBatch.setLastSequenceId(commitId);
                    wal.log(writeBatch);
                }
                applyLocalStore(writeBatch);
            }

            transactionManager.commitTransaction(txId);
            LOG.info("Transaction {}", this);
        } finally {
            transactionManager.transactionWriteUnLock();
        }
    }

    private void applyLocalStore(WriteBatch writeBatch) {
        storeManager.put(writeBatch);
    }

    public void rollback() {
        transactionManager.rollBackTransaction(txId);
        LOG.info("Transaction {}", this);
    }

    protected boolean isCommit() {
        return TransactionState.Commit.equals(this.transactionState);
    }

    public KeySet getWriteSet() {
        return writeSet;
    }

    public KeySet getReadSet() {
        return readSet;
    }

    public void setTransactionState(TransactionState state) {
        this.transactionState = state;
    }

    public void setCommittedId(long committedId) {
        this.commitId = committedId;
    }

    public long getCommitId() {
        return commitId;
    }

    public WriteBatch getWriteBatch() {
        return writeBatch;
    }

    public Wal getWal() {
        return wal;
    }

    public StoreManager getStoreManager() {
        return storeManager;
    }

    public long getReadPoint() {
        return readPoint;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "txId=" + txId +
                ", commitId=" + commitId +
                ", transactionState=" + transactionState +
                ", writeSet=" + writeSet +
                ", readSet=" + readSet +
                ", checkTransactions=" + checkTransactions +
                '}';
    }

    private List<Transaction> checkTransactions = new ArrayList<>();
    public void addCheckTransaction(Transaction otherCommittedTransaction) {
        checkTransactions.add(otherCommittedTransaction);
    }
}
