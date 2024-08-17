package org.minbase.server.transaction;



import org.minbase.common.exception.TransactionException;

import org.minbase.server.kv.KeyValue;
import org.minbase.server.transaction.store.WriteBatch;
import org.minbase.server.table.Table;
import org.minbase.server.transaction.store.TransactionStore;
import org.minbase.server.transaction.table.TxTable;
import org.minbase.server.wal.Wal;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Transaction implements org.minbase.common.transaction.Transaction {
    protected long txId;
    private long commitId;
    protected TransactionStore localStore;
    protected TransactionState transactionState;

    protected Map<String, Table> tables;
    private Wal wal;

    private Set<byte[]> writeSet;
    private Set<byte[]> readSet;

    public Transaction(long transactionId) {
        this.txId = transactionId;
        this.transactionState = TransactionState.Active;
        this.localStore = new TransactionStore();
        this.writeSet = new HashSet<>();
        this.readSet = new HashSet<>();
    }

    @Override
    public long txId() {
        return txId;
    }

    @Override
    public TxTable getTable(String tableName) {
        return new TxTable( tables.get(tableName), this);
    }

    public long getTxId() {
        return txId;
    }

    public TransactionState getTransactionState() {
        return transactionState;
    }


    public synchronized void commit() throws TransactionException {
        if (!TransactionManager.validateTransaction(txId)) {
            throw new TransactionException("validate fail");
        }

        WriteBatch writeBatch = localStore.getWriteBatch();
        if (!writeBatch.isEmpty()) {
            writeBatch.setSequenceId(commitId);
            wal.log(writeBatch);
            applyLocalStore(localStore);
        }

        TransactionManager.commitTransaction(txId);
        localStore = null;
    }

    private void applyLocalStore(TransactionStore localStore) {
        WriteBatch writeBatch = localStore.getWriteBatch();
        writeBatch.setSequenceId(commitId);
        
        for (String table : writeBatch.getTables()) {
            List<KeyValue> keyValues = writeBatch.getKeyValues(table);
            for (KeyValue keyValue : keyValues) {
                tables.get(table).getMinStore().put(keyValue.getKey(), keyValue.getValue());
            }
        }
    }

    public void rollback() {
        TransactionManager.rollBackTransaction(txId);
        localStore = null;
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

    public void setTables(Map<String, org.minbase.server.table.Table> tables) {
        this.tables = tables;
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

    public void setCommitId(long commitId) {
        this.commitId = commitId;
    }

    public long getCommitId() {
        return commitId;
    }
}
