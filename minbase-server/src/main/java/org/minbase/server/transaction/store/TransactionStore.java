package org.minbase.server.transaction.store;



import org.minbase.common.operation.Put;
import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.mem.MemStore;
import org.minbase.server.op.Key;
import org.minbase.server.op.Value;
import org.minbase.server.op.WriteBatch;
import org.minbase.common.utils.ByteUtil;

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

    public Value get(byte[] key) {
        return table.get(key);
    }

    public void put(byte[] key, byte[] value) {
//        Value put = Value.Put(value);
//        table.put(key, put);
//        writeBatch.put(key,put);
    }

    public void delete(byte[] key) {
//        final Value delete = Value.Delete();
//        table.put(key, delete);
//        writeBatch.put(key,delete);
    }

    public KeyValueIterator iterator(Key startKey, Key endKey) {

    }

    public Map<String, MemStore> getMemStores() {
        return memStores;
    }

    public void put(Put put) {
    }
}
