package org.minbase.server.table;



import org.minbase.common.exception.TransactionException;
import org.minbase.common.table.Table;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.WriteBatch;
import org.minbase.server.kv.store.StoreManager;
import org.minbase.server.table.wal.Wal;

import java.util.*;

public class Transaction implements org.minbase.common.table.transaction.Transaction {
    protected long txId;
    private long commitId;
    protected TransactionState transactionState;
    protected StoreManager storeManager;
    protected TransactionManager transactionManager;
    protected Set<byte[]> writeSet;
    protected Set<byte[]> readSet;
    private Map<String, TransactionTable> txTables = new HashMap<>();
    private WriteBatch writeBatch;
    private Wal wal;

    public Transaction(long transactionId, TableManager tableManager) {
        this.txId = transactionId;
        this.transactionState = TransactionState.Active;
        this.writeBatch = new WriteBatch();
        this.writeSet = new HashSet<>();
        this.readSet = new HashSet<>();
        this.storeManager = tableManager.getStoreManager();
        this.transactionManager = tableManager.getTransactionManager();
        this.wal = tableManager.getWal();
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


    public synchronized void commit() throws TransactionException {
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
    }

    private void applyLocalStore(WriteBatch writeBatch) {
        storeManager.put(writeBatch);
    }

    public void rollback() {
        transactionManager.rollBackTransaction(txId);
    }

    protected boolean isCommit() {
        return TransactionState.Commit.equals(this.transactionState);
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "transactionId=" + txId +
                ", transactionState=" + transactionState +
                '}';
    }

    public Set<byte[]> getWriteSet() {
        return writeSet;
    }

    public Set<byte[]> getReadSet() {
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

    public void applyLogForRecovery(String tableName, List<KeyValue> keyValues) {
        TransactionTable table = (TransactionTable) getTable(tableName);
        for (KeyValue keyValue : keyValues) {
            table.applyLog(keyValue);
        }
    }

    public void commitForRecovery(long commitId) {
        applyLocalStore(writeBatch);
        transactionManager.commitTransaction(txId);
    }

    public void rollbackForRecovery() {
        transactionManager.rollBackTransaction(txId);
    }
}
