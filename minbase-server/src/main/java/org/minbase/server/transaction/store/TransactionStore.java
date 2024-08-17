package org.minbase.server.transaction.store;



import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.kv.Value;
import org.minbase.server.mem.MemStore;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.utils.ValueUtils;

import java.util.HashMap;
import java.util.Map;

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

    public KeyValue get(String tableName, Key key) {
        MemStore memStore = memStores.get(tableName);
        return memStore.get(key);
    }

    public void delete(String tableName, Key key) {
        Value value = ValueUtils.Delete();
        writeBatch.add(tableName, new KeyValue(key, value));
        MemStore memStore = memStores.get(tableName);
        memStore.put(key, value);
    }

    public KeyValueIterator iterator(String tableName, Key startKey, Key endKey) {
        MemStore memStore = memStores.get(tableName);
        return memStore.iterator(startKey, endKey);
    }

    public Map<String, MemStore> getMemStores() {
        return memStores;
    }

    public void put(String tableName, Key key, Value value) {
        KeyValue keyValue = new KeyValue(key, value);
        writeBatch.add(tableName, keyValue);
        MemStore memStore = memStores.get(tableName);
        memStore.put(key, value);
    }
}
