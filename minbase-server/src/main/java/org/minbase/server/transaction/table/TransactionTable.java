package org.minbase.server.transaction.table;

import org.minbase.common.operation.ColumnValues;
import org.minbase.common.operation.Delete;
import org.minbase.common.operation.Get;
import org.minbase.common.operation.Put;
import org.minbase.common.table.Table;
import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.iterator.MergeIterator;
import org.minbase.server.minstore.MinStore;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.op.RowTacker;
import org.minbase.server.transaction.Transaction;
import org.minbase.server.transaction.store.TransactionStore;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class TransactionTable implements Table {
    private String tableName;
    private MinStore minStore;
    private long txId;
    protected TransactionStore localStore;
    private Transaction transaction;
    private Set<byte[]> writeSet;
    private Set<byte[]> readSet;

    public TransactionTable(long txId, Transaction transaction) {

    }

    @Override
    public String name() {
        return tableName;
    }

    @Override
    public ColumnValues get(Get get) {
        readSet.add(get.getKey());

        KeyValueIterator iterator1 = minStore.iterator(Key.minKey(get.getKey()), Key.maxKey(get.getKey()));
        KeyValueIterator iterator2 = localStore.iterator(Key.minKey(get.getKey()), Key.maxKey(get.getKey()));
        List<KeyValueIterator> keyValueIteratorList = new ArrayList<>();
        keyValueIteratorList.add(iterator1);
        keyValueIteratorList.add(iterator2);
        MergeIterator iterator = new MergeIterator(keyValueIteratorList);

        KeyValue keyValue;
        try {
            RowTacker tacker = new RowTacker(Key.latestKey(get.getKey()), new HashSet<>(get.getColumns()));
            while (iterator.isValid()) {
                final KeyValue tmp = iterator.value();
                tacker.track(tmp);
                if (tacker.shouldStop()) {
                    keyValue = tacker.getKeyValue();
                    return keyValue.getValue().columnValues();
                }
                iterator.nextInnerKey();
            }
            keyValue = tacker.getKeyValue();
        } finally {
            iterator.close();
        }
        return keyValue.getValue().columnValues();
    }

    @Override
    public void put(Put put) {
        writeSet.add(put.getKey());

        localStore.put(put);
    }

    @Override
    public boolean checkAndPut(byte[] checkKey, byte[] column, byte[] checkValue, Put put) {
        readSet.add(checkKey);
        writeSet.add(put.getKey());

        return false;
    }

    @Override
    public void delete(Delete delete) {
        writeSet.add(delete.getKey());

        localStore.delete();
    }
}
