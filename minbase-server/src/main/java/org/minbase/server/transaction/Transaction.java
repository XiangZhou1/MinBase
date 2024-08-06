package org.minbase.server.transaction;



import org.minbase.common.exception.TransactionException;
import org.minbase.server.minstore.MinStore;
import org.minbase.server.table.TableImpl;
import org.minbase.server.transaction.lock.KeyLock;
import org.minbase.server.transaction.store.TransactionStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class Transaction {
    protected long transactionId;
    protected TransactionStore writeBatchTable;

    protected TransactionState transactionState;
    protected KeyLock keyLock;
    protected long snapShot;
    protected List<Transaction> activeTransactions = new ArrayList<>();
    protected Map<String, TableImpl> tables;

    public Transaction(long transactionId) {
        this.transactionId = transactionId;
        this.transactionState = TransactionState.Active;
        this.writeBatchTable = new TransactionStore();
    }

    public long getTransactionId() {
        return transactionId;
    }

    public TransactionState getTransactionState() {
        return transactionState;
    }

    public void setTransactionState(TransactionState transactionState) {
        this.transactionState = transactionState;
    }

    public KeyLock getKeyLock() {
        return keyLock;
    }

    public void setKeyLock(KeyLock keyLock) {
        this.keyLock = keyLock;
    }

    public long getSnapShot() {
        return snapShot;
    }

    public void setSnapShot(long snapShot) {
        this.snapShot = snapShot;
    }

    public List<Transaction> getActiveTransactions() {
        return activeTransactions;
    }

    public void setActiveTransactions(List<Transaction> activeTransactions) {
        this.activeTransactions = activeTransactions;
    }

    public void commit() throws TransactionException {
        synchronized (Transaction.class){
            commitImpl();
        }
    }

    public void addCheckTransaction(Transaction transaction) {
        activeTransactions.add(transaction);
    }

    protected abstract void commitImpl();

    public abstract void rollback();


    protected boolean isCommit() {
        return TransactionState.Commit.equals(this.transactionState);
    }


    @Override
    public String toString() {
        return "Transaction{" +
                "transactionId=" + transactionId +
                ", transactionState=" + transactionState +
                ", keyLock=" + keyLock +
                ", snapShot=" + snapShot +
                '}';
    }

    public void setTables(Map<String, TableImpl> tables) {
        this.tables = tables;
    }
}
