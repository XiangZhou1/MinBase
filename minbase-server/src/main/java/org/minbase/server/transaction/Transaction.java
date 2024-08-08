package org.minbase.server.transaction;



import org.minbase.common.exception.TransactionException;
import org.minbase.common.table.Table;
import org.minbase.server.op.WriteBatch;
import org.minbase.server.table.TableImpl;
import org.minbase.server.transaction.store.TransactionStore;
import org.minbase.server.transaction.table.TransactionTable;
import org.minbase.server.wal.Wal;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

public class Transaction implements org.minbase.common.transaction.Transaction {
    protected long txId;
    private long commitId;
    protected TransactionStore localStore;
    protected TransactionState transactionState;

    protected Map<String, TableImpl> tables;
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
    public Table getTable(String tableName) {
        return new TransactionTable(txId, this);
    }

    public long getTxId() {
        return txId;
    }

    public TransactionState getTransactionState() {
        return transactionState;
    }


    public synchronized void commit() throws TransactionException {
        long commitId = TransactionManager.getCommitId();
        ConcurrentSkipListMap<Long, Transaction> commitedTransactions = TransactionManager.getCommitedTransactions();
        if (writeSet.isEmpty() && !readSet.isEmpty()) {
            WriteBatch writeBatch = localStore.getWriteBatch();
            writeBatch.setSequenceId(commitId);
            wal.log(writeBatch);
            localStore.getWriteBatch();
            this.commitId = commitId;
        } else if (!writeSet.isEmpty() && readSet.isEmpty()) {
            this.commitId = commitId;
            transactionState = TransactionState.Commit;
        } else {

            for (Map.Entry<Long, Transaction> entry : commitedTransactions.entrySet()) {
                Long id = entry.getKey();
                Transaction transaction = entry.getValue();
                if (id < commitId && transaction.getTxId() > this.txId) {
                    boolean isConflict = checkConflict(transaction);
                    if (isConflict) {
                        throw new TransactionException();
                    }
                }
            }

            localStore.getWriteBatch();
            this.commitId = commitId;
        }

        transactionState = TransactionState.Commit;

        ConcurrentSkipListMap<Long, Transaction> activeTransactions = TransactionManager.getActiveTransactions();
        activeTransactions.remove(this.txId);
        commitedTransactions.put(txId, this);
        localStore = null;
    }

    private boolean checkConflict(Transaction transaction) {
        for (byte[] bytes : readSet) {
            if (transaction.writeSet.contains(bytes)) {
                return true;
            }
        }
        return false;
    }

    public void rollback() {
        transactionState = TransactionState.Rollback;
        ConcurrentSkipListMap<Long, Transaction> activeTransactions = TransactionManager.getActiveTransactions();
        activeTransactions.remove(this.txId);
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

    public void setTables(Map<String, TableImpl> tables) {
        this.tables = tables;
    }
}
