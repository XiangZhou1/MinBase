package org.minbase.server.table;

import org.minbase.common.table.op.ColumnValues;
import org.minbase.common.table.op.Delete;
import org.minbase.common.table.op.Get;
import org.minbase.common.table.op.Put;
import org.minbase.common.table.Table;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.iterator.KeyValueIterator;
import org.minbase.server.kv.iterator.MergeIterator;
import org.minbase.server.kv.store.Scanner;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.store.StoreManager;
import org.minbase.server.table.wal.Wal;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;


public class TransactionTable implements Table {
    private String tableName;
    protected TransactionTableStore localStore;
    private Set<byte[]> writeSet;
    private Set<byte[]> readSet;
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
        readSet.add(get.getKey());

        List<byte[]> columns = get.getColumns();
        byte[] key = get.getKey();
        columns.sort(ByteUtil.BYTE_ORDER_COMPARATOR);

        TableKey tableKeyFirst = new TableKey(key, columns.get(0));
        TableKey tableKeyLast = new TableKey(key, columns.get(columns.size() - 1));

        Key startKey = new Key(tableKeyLast.encode(), Long.MAX_VALUE);
        Key endKey = new Key(tableKeyFirst.encode(), 0);
        Scanner scan = storeManager.scan(tableName, startKey, endKey);
        Scanner localScanner = new Scanner(localStore.iterator(startKey, endKey), Long.MAX_VALUE);
        List<KeyValueIterator> iterators = new ArrayList<>();
        iterators.add(scan);
        iterators.add(localScanner);
        Scanner scanner = new Scanner(new MergeIterator(iterators), Long.MAX_VALUE);

        ColumnValues columnValues = new ColumnValues();
        while (scanner.hasNext()) {
            KeyValue keyValue = scanner.next();
            if (keyValue == null) {
                continue;
            }
            TableKey tableKey = new TableKey();
            tableKey.decode(keyValue.getKey().getInternalKey());
            byte[] column = tableKey.getColumn();
            if (columns.contains(column)) {
                columnValues.set(column, keyValue.getValue().getValue());
            }
        }
        return columnValues;
    }

    @Override
    public void put(Put put) {
        writeSet.add(put.getKey());
        List<KeyValue> keyValues = OpUtil.fromPut(put);
        for (KeyValue keyValue : keyValues) {
            localStore.put(keyValue);
        }
    }

    @Override
    public boolean checkAndPut(byte[] checkKey, byte[] column, byte[] checkValue, Put put) {
        readSet.add(checkKey);
        writeSet.add(put.getKey());

        Get get = new Get(checkKey);
        get.addColumn(column);
        ColumnValues columnValues = get(get);
        byte[] value = columnValues.get(column);
        if (value == checkValue) {
            put(put);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void delete(Delete delete) {
        writeSet.add(delete.getKey());
        List<KeyValue> keyValues = OpUtil.fromDelete(delete);
        for (KeyValue keyValue : keyValues) {
            localStore.put(keyValue);
        }
    }

    public void applyLog(KeyValue keyValue) {
        localStore.put(keyValue);
    }
}
