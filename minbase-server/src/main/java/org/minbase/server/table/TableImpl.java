package org.minbase.server.table;

import org.minbase.common.exception.TransactionException;
import org.minbase.common.table.op.ColumnValues;
import org.minbase.common.table.op.Delete;
import org.minbase.common.table.op.Get;
import org.minbase.common.table.op.Put;
import org.minbase.common.table.Table;
import org.minbase.server.kv.store.Store;
import org.minbase.server.table.transaction.Transaction;
import org.minbase.server.table.transaction.TransactionManager;

import java.util.HashMap;
import java.util.Map;

public class TableImpl implements Table {
    String tableName;
    Store store;
    Map<String, TableImpl> selfTables;

    public TableImpl(String tableName, Store store) {
        this.tableName = tableName;
        this.store = store;
        this.selfTables = new HashMap<>();
        this.selfTables.put(tableName, this);
    }

    public Store getMinStore() {
        return store;
    }

    @Override
    public String name() {
        return tableName;
    }

    @Override
    public ColumnValues get(Get get) {
        Transaction transaction = TransactionManager.newTransaction(selfTables);
        Table table = transaction.getTable(tableName);
        ColumnValues columnValues = table.get(get);
        transaction.commit();
        return columnValues;
    }

    @Override
    public void put(Put put) {
        Transaction transaction = TransactionManager.newTransaction(selfTables);
        Table table = transaction.getTable(tableName);
        table.put(put);
        transaction.commit();
    }

    @Override
    public boolean checkAndPut(byte[] checkKey, byte[] column, byte[] checkValue, Put put) {
        Transaction transaction = TransactionManager.newTransaction(selfTables);
        try {
            Table table = transaction.getTable(tableName);
            if (table.checkAndPut(checkKey, column, checkValue, put)) {
                transaction.commit();
                return true;
            } else {
                transaction.rollback();
                return false;
            }
        } catch (TransactionException e) {
            transaction.rollback();
            return false;
        }
    }

    @Override
    public void delete(Delete delete) {
        Transaction transaction = TransactionManager.newTransaction(selfTables);
        Table table = transaction.getTable(tableName);
        table.delete(delete);
        transaction.commit();
    }
}
