package org.minbase.server.table;

import org.minbase.common.exception.TransactionException;
import org.minbase.common.operation.ColumnValues;
import org.minbase.common.operation.Delete;
import org.minbase.common.operation.Get;
import org.minbase.common.operation.Put;
import org.minbase.common.table.Table;
import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.minstore.MinStore;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.op.RowTacker;
import org.minbase.server.transaction.Transaction;
import org.minbase.server.transaction.TransactionManager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TableImpl implements Table {
    String tableName;
    MinStore minStore;
    Map<String, TableImpl> selfTables;

    public TableImpl(String tableName, MinStore minStore) {
        this.tableName = tableName;
        this.minStore = minStore;
        this.selfTables = new HashMap<>();
        this.selfTables.put(tableName, this);
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
