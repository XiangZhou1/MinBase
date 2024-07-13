package org.minbase.server.transaction;



import org.minbase.server.constant.Constants;
import org.minbase.server.exception.TransactionException;
import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.iterator.MergeIterator;
import org.minbase.server.lsmStorage.LsmStorage;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.op.Value;
import org.minbase.server.transaction.lock.KeyLock;
import org.minbase.server.transaction.table.TransactionTable;
import org.minbase.server.transaction.table.TransactionTableIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class Transaction {
    protected long transactionId;
    protected TransactionTable writeBatchTable;
    protected LsmStorage lsmStorage;
    protected TransactionState transactionState;
    protected KeyLock keyLock;
    protected long snapShot;
    protected List<Transaction> activeTransactions = new ArrayList<>();

    public Transaction(long transactionId) {
        this.transactionId = transactionId;
        this.transactionState = TransactionState.Active;
        this.writeBatchTable = new TransactionTable();
    }

    public long getTransactionId() {
        return transactionId;
    }

    public void setLsmStorage(LsmStorage lsmStorage) {
        this.lsmStorage = lsmStorage;
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

    // 当前读
    public KeyValue get(byte[] key) {
        Value value = writeBatchTable.get(key);
        if (value != null) {
            return new KeyValue(new Key(key, Constants.LATEST_VERSION), value);
        }

        return lsmStorage.getInner(new Key(key, snapShot));
    }


    public KeyValue getForUpdate(byte[] key) {
        keyLock.readLock(key);
        return get(key);
    }

    public void put(byte[] key, byte[] value) {
        keyLock.writeLock(key);
        writeBatchTable.put(key, value);
    }

    public void delete(byte[] key) {
        keyLock.writeLock(key);
        writeBatchTable.delete(key);
    }

    protected boolean isCommit() {
        return TransactionState.Commit.equals(this.transactionState);
    }


    public KeyValueIterator scan(byte[] startKey, byte[] endKey) {
        TransactionTableIterator iterator1 = new TransactionTableIterator(writeBatchTable, startKey, endKey);
        KeyValueIterator iterator2 = lsmStorage.scan(startKey, endKey);
        return new MergeIterator(Arrays.asList(iterator1, iterator2));
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
}
