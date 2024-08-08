package org.minbase.server.table;

import org.minbase.common.exception.TransactionException;
import org.minbase.common.operation.ColumnValues;
import org.minbase.common.operation.Delete;
import org.minbase.common.operation.Get;
import org.minbase.common.operation.Put;
import org.minbase.common.table.Table;
import org.minbase.server.minstore.MinStore;
import org.minbase.server.op.KeyValue;
import org.minbase.server.transaction.Transaction;
import org.minbase.server.transaction.TransactionManager;

public class TableImpl implements Table {
    String tableName;
    MinStore minStore;

    public TableImpl(String tableName, MinStore minStore) {
        this.tableName = tableName;
        this.minStore = minStore;
    }


    public MinStore getMinStore() {
        return minStore;
    }

    @Override
    public String name() {
        return tableName;
    }

    @Override
    public ColumnValues get(Get get) {
        Transaction transaction = TransactionManager.newTransaction(null);

        Table table = transaction.getTable(tableName);
        ColumnValues columnValues = table.get(get);
        transaction.commit();


        KeyValue keyValue = minStore.get(get);
        return keyValue.getValue().columnValues();
    }

    @Override
    public void put(Put put) {
        Transaction transaction = TransactionManager.newTransaction(null);
        Table table = transaction.getTable(tableName);
        table.put(put);
        transaction.commit();
    }

    @Override
    public boolean checkAndPut(byte[] checkKey, byte[] column, byte[] checkValue, Put put) {
        Transaction transaction = TransactionManager.newTransaction(null);
        try {
            Table table = transaction.getTable(tableName);
            table.put(put);
            transaction.commit();
            return true;
        } catch (TransactionException e) {
            transaction.rollback();
            return false;
        }
    }

    @Override
    public void delete(Delete delete) {
        Transaction transaction = TransactionManager.newTransaction(null);
        Table table = transaction.getTable(tableName);
        table.delete(delete);
        transaction.commit();
    }
}
