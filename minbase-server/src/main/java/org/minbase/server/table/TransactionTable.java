package org.minbase.server.table;

import org.minbase.common.table.op.ColumnValues;
import org.minbase.common.table.op.Delete;
import org.minbase.common.table.op.Get;
import org.minbase.common.table.op.Put;
import org.minbase.common.table.ClientTable;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.iterator.KeyValueIterator;
import org.minbase.server.kv.iterator.MergeIterator;
import org.minbase.server.kv.store.Scanner;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.store.StoreManager;

import java.util.ArrayList;
import java.util.List;


public class TransactionTable implements Table {
    private String tableName;
    protected TransactionTableStore localStore;
    private KeySet writeSet;
    private KeySet readSet;
    private StoreManager storeManager;
    private Transaction transaction;

    public TransactionTable(String tableName, Transaction transaction) {
        this.tableName = tableName;
        this.transaction = transaction;
        this.writeSet = transaction.getWriteSet();
        this.readSet = transaction.getReadSet();
        this.storeManager = transaction.getStoreManager();
        this.localStore = new TransactionTableStore(tableName, transaction.getWriteBatch());
    }

    @Override
    public String name() {
        return tableName;
    }

    @Override
    public ColumnValues get(Get get) {
        readSet.put(tableName, get.getKey());

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
        Scanner scan = storeManager.scan(tableName, startKey, endKey, transaction.getReadPoint());
        Scanner localScanner = new Scanner(localStore.iterator(startKey, endKey), Long.MAX_VALUE);
        List<KeyValueIterator> iterators = new ArrayList<>();
        iterators.add(scan);
        iterators.add(localScanner);
        Scanner scanner = new Scanner(new MergeIterator(iterators), Long.MAX_VALUE);

        RawTracker rawTracker = new RawTracker(columns);
        ColumnValues result = rawTracker.tracker(scanner);
        return result;
    }

    @Override
    public void put(Put put) {
        writeSet.put(tableName, put.getKey());
        List<KeyValue> keyValues = OpUtil.fromPut(put);
        for (KeyValue keyValue : keyValues) {
            localStore.put(keyValue);
        }
    }

    @Override
    public boolean checkAndPut(byte[] checkKey, byte[] column, byte[] checkValue, Put put) {
        readSet.put(tableName, checkKey);
        writeSet.put(tableName, put.getKey());

        Get get = new Get(checkKey);
        get.addColumn(column);
        ColumnValues columnValues = get(get);
        byte[] value = columnValues.get(column);
        boolean equal = (value == null && checkValue == null) ||
                (value != null && checkValue != null && ByteUtil.byteEqual(value, checkValue));
        if (equal) {
            put(put);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void delete(Delete delete) {
        writeSet.put(tableName, delete.getKey());
        List<KeyValue> keyValues = OpUtil.fromDelete(delete);
        for (KeyValue keyValue : keyValues) {
            localStore.put(keyValue);
        }
    }

    public void applyLog(KeyValue keyValue) {
        localStore.put(keyValue);
    }
}
