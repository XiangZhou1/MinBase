package org.minbase.server.table;

import org.minbase.common.conf.Configuration;
import org.minbase.server.kv.iterator.KeyValueIterator;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.store.MemStore;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.WriteBatch;
import org.minbase.server.kv.store.MemStoreIterator;

public class TransactionTableStore {
    private String tableName;
    WriteBatch writeBatch;
    MemStore localStore;

    public TransactionTableStore(String tableName, WriteBatch writeBatch) {
        this.tableName = tableName;
        this.writeBatch = writeBatch;
        this.localStore = new MemStore(new Configuration());
    }

    public WriteBatch getWriteBatch() {
        return writeBatch;
    }

    public void put(KeyValue keyValue) {
        localStore.put(keyValue);
        writeBatch.add(tableName, keyValue);
    }

    public KeyValueIterator iterator(Key startKey, Key endKey) {
        return new MemStoreIterator(localStore, startKey, endKey);
    }
}
