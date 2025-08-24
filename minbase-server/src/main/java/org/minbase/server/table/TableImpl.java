package org.minbase.server.table;

import org.minbase.common.table.TableInfo;
import org.minbase.common.table.op.ColumnValues;
import org.minbase.common.table.op.Delete;
import org.minbase.common.table.op.Get;
import org.minbase.common.table.op.Put;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.store.Scanner;
import org.minbase.server.kv.store.Store;
import org.minbase.server.kv.store.StoreManager;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class TableImpl implements Table {
    String tableName;
    TableInfo tableInfo;
    Store store;
    StoreManager storeManager;
    TableManager tableManager;
    TransactionManager transactionManager;

    boolean droped = false;

    public TableImpl(TableInfo tableInfo, TableManager tableManager) {
        this.tableInfo = tableInfo;
        this.tableName = tableInfo.getName();
        this.store = tableManager.getStoreManager().getStore(tableName);
        this.tableManager = tableManager;
        this.storeManager = tableManager.getStoreManager();
        this.transactionManager = tableManager.getTransactionManager();
    }

    public Store getMinStore() {
        return store;
    }

    @Override
    public String name() {
        return tableName;
    }

    /**
     * 无需进行事务
     *
     * @param get
     * @return
     */
    @Override
    public ColumnValues get(Get get) {
        List<byte[]> columns = get.getColumns();
        byte[] key = get.getKey();
        TableKey tableKeyFirst = null;
        TableKey tableKeyLast = null;
        if (!columns.isEmpty()) {
            columns.sort(ByteUtil.BYTE_ORDER_COMPARATOR);
            tableKeyFirst = new TableKey(key, columns.get(0));
            tableKeyLast = new TableKey(key, columns.get(columns.size() - 1));
        } else {
            tableKeyFirst = new TableKey(key, new byte[]{Byte.MIN_VALUE});
            tableKeyLast = new TableKey(key, new byte[]{Byte.MAX_VALUE});
        }

        Key startKey = new Key(tableKeyFirst.encode(), Long.MAX_VALUE);
        Key endKey = new Key(tableKeyLast.encode(), 0);
        Scanner scan = storeManager.scan(tableName, startKey, endKey);
        RawTracker rawTracker = new RawTracker(columns);
        return rawTracker.tracker(scan);
    }

    @Override
    public void put(Put put) throws IOException {
        Transaction transaction = transactionManager.newTransaction();
        Table table = transaction.getTable(tableName);
        table.put(put);
        transaction.commit();
    }

    @Override
    public boolean checkAndPut(byte[] checkKey, byte[] column, byte[] checkValue, Put put) {
        Transaction transaction = transactionManager.newTransaction();
        try {
            Table table = transaction.getTable(tableName);
            if (table.checkAndPut(checkKey, column, checkValue, put)) {
                transaction.commit();
                return true;
            } else {
                transaction.rollback();
                return false;
            }
        } catch (IOException e) {
            transaction.rollback();
            return false;
        }
    }

    @Override
    public void delete(Delete delete) throws IOException {
        Transaction transaction = transactionManager.newTransaction();
        Table table = transaction.getTable(tableName);
        if (delete.getColumns().isEmpty()) {
            Iterator<String> iterator = table.getTableInfo().getColumns().iterator();
            while (iterator.hasNext()) {
                String column = iterator.next();
                delete.addColumn(ByteUtil.toBytes(column));
            }
        }
        table.delete(delete);
        transaction.commit();
    }

    public TableInfo getTableInfo() {
        return tableInfo;
    }
}
