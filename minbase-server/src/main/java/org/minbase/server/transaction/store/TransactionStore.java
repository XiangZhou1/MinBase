package org.minbase.server.transaction.store;



import org.minbase.common.operation.Delete;
import org.minbase.common.operation.Put;
import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.mem.MemStore;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.op.Value;
import org.minbase.server.op.WriteBatch;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.utils.KeyValueUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class TransactionStore {
    WriteBatch writeBatch;

    Map<String, MemStore> memStores;

    public TransactionStore() {
        this.writeBatch = new WriteBatch();
        this.memStores = new HashMap<>();
    }

    public WriteBatch getWriteBatch() {
        return writeBatch;
    }


    public void delete(String tableName, Delete delete) {
        KeyValue keyValue = KeyValueUtil.toKeyValue(delete);
        writeBatch.add(tableName, keyValue);
        MemStore memStore = memStores.get(tableName);
        memStore.put(keyValue.getKey(), keyValue.getValue());
    }

    public KeyValueIterator iterator(String tableName, Key startKey, Key endKey) {
        return null;
    }

    public Map<String, MemStore> getMemStores() {
        return memStores;
    }

    public void put(String tableName, Put put) {
        KeyValue keyValue = KeyValueUtil.toKeyValue(put);
        writeBatch.add(tableName, keyValue);
        MemStore memStore = memStores.get(tableName);
        memStore.put(keyValue.getKey(), keyValue.getValue());
    }
}
