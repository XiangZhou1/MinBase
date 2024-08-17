package org.minbase.server.transaction.table;

import org.minbase.common.op.ColumnValues;
import org.minbase.common.op.Delete;
import org.minbase.common.op.Get;
import org.minbase.common.op.Put;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.iterator.MergeIterator;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.table.Table;
import org.minbase.server.table.kv.InternalKey;
import org.minbase.server.table.kv.RowTacker;
import org.minbase.server.transaction.Transaction;
import org.minbase.server.transaction.store.TransactionStore;
import org.minbase.server.utils.ValueUtils;

import java.util.*;


public class TxTable implements org.minbase.common.table.Table {
    private long txId;
    private Table rawTable;
    protected TransactionStore localStore;
    private Set<byte[]> writeSet;
    private Set<byte[]> readSet;

    public TxTable(Table rawTable, Transaction transaction ) {
        this.rawTable = rawTable;
        this.writeSet = transaction.getWriteSet();
        this.readSet = transaction.getReadSet();
        this.localStore = new TransactionStore();
        this.txId = transaction.getTxId();
    }

    @Override
    public String name() {
        return rawTable.name();
    }

    @Override
    public ColumnValues get(Get get) {
        get.setSequenceId(txId);
        readSet.add(ByteUtil.toBytes(get.getKey()));

        if (get.getColumns().size() == 1 || rawTable.getColumns().size() == 1) {
            List<String> columns = get.getColumns().isEmpty() ? rawTable.getColumns() : get.getColumns();
            InternalKey internalKey = new InternalKey(ByteUtil.toBytes(get.getKey()), ByteUtil.toBytes(columns.get(0)), get.getSequenceId());
            KeyValue keyValue = localStore.get(rawTable.name(), internalKey);
            ColumnValues columnValues = new org.minbase.server.table.kv.ColumnValues();
            if (keyValue != null) {
                org.minbase.server.kv.Value value = keyValue.getValue();
                if (value.isDelete()) {
                    return columnValues;
                } else {
                    columnValues.set(ByteUtil.toBytes(internalKey.getColumn()), value.getData());
                }
            }
            return rawTable.get(get);
        }

        Key minKey;
        Key maxKey;
        List<String> columns;
        if (!get.getColumns().isEmpty()) {
            columns = get.getColumns();
            minKey = new InternalKey(ByteUtil.toBytes(get.getKey()), ByteUtil.toBytes(columns.get(0)), Long.MAX_VALUE);
            maxKey = new InternalKey(ByteUtil.toBytes(get.getKey()), ByteUtil.toBytes(columns.get(columns.size() - 1)), get.getSequenceId()+1);
        } else {
            columns = rawTable.getColumns();
            minKey = new InternalKey(ByteUtil.toBytes(get.getKey()), ByteUtil.toBytes(columns.get(0)), Long.MAX_VALUE);
            maxKey = new InternalKey(ByteUtil.toBytes(get.getKey()), ByteUtil.toBytes(columns.get(columns.size() - 1)), get.getSequenceId()+1);
        }

        List<KeyValueIterator> keyValueIteratorList = new ArrayList<>();
        KeyValueIterator iterator1 = rawTable.iterator(minKey, maxKey);
        KeyValueIterator iterator2 = localStore.iterator(rawTable.name(), minKey, maxKey);
        keyValueIteratorList.add(iterator2);
        keyValueIteratorList.add(iterator1);
        KeyValueIterator iterator = new MergeIterator(keyValueIteratorList);

        List<KeyValue> keyValues;
        ColumnValues columnValues = new org.minbase.server.table.kv.ColumnValues();
        try {
            RowTacker tacker = new RowTacker(get.getKey(), new HashSet<>(columns), get.getSequenceId());
            while (iterator.isValid()) {
                KeyValue tmp = iterator.value();
                tacker.track(tmp);
                if (tacker.shouldStop()) {
                    keyValues = tacker.getKeyValues();
                    continue;
                }
                iterator.nextInnerKey();
            }
            keyValues = tacker.getKeyValues();
        } finally {
            iterator.close();
        }

        for (KeyValue keyValue : keyValues) {
            InternalKey key = (InternalKey) keyValue.getKey();
            columnValues.set(ByteUtil.toBytes(key.getColumn()), keyValue.getValue().getData());
        }
        return columnValues;
    }

    @Override
    public void put(Put put) {
        put.setSequenceId(txId);
        writeSet.add(put.getKey());

        byte[] userKey = put.getKey();
        for (Map.Entry<byte[], byte[]> entry : put.getColumnValues().entrySet()) {
            byte[] column = entry.getKey();
            byte[] data = entry.getValue();
            localStore.put(rawTable.name(), new InternalKey(userKey, column, put.getSequenceId()), ValueUtils.Put(data));
        }
    }

    @Override
    public boolean checkAndPut(byte[] checkKey, byte[] column, byte[] checkValue, Put put) {
        readSet.add(checkKey);
        writeSet.add(put.getKey());

        List<String> columns = new ArrayList<>();
        columns.add(new String(column));
        Get get = new Get(new String(checkKey),columns);
        get.setSequenceId(txId);

        ColumnValues columnValues = get(get);
        byte[] data = columnValues.get(column);
        put.setSequenceId(txId);
        if(data != null && checkValue != null && ByteUtil.byteEqual(data, checkValue)){
            put(put);
            return true;
        }
        if (data == null && checkValue == null){
            put(put);
            return true;
        }
        return false;
    }

    @Override
    public void delete(Delete delete) {
        writeSet.add(ByteUtil.toBytes(delete.getKey()));
        delete.setSequenceId(txId);

        List<String> columns;
        if (!delete.getColumns().isEmpty()) {
            columns = delete.getColumns();
        } else {
            columns = rawTable.getColumns();
        }

        for (String column : columns) {
            InternalKey key = new InternalKey(ByteUtil.toBytes(delete.getKey()), ByteUtil.toBytes(column), delete.getSequenceId());
            localStore.delete(rawTable.name(), key);
        }
    }
}
