package org.minbase.server.transaction.store;



import org.minbase.server.op.Value;
import org.minbase.server.op.WriteBatch;
import org.minbase.common.utils.ByteUtil;

import java.util.concurrent.ConcurrentSkipListMap;

public class TransactionStore {
    WriteBatch writeBatch;
    ConcurrentSkipListMap<byte[], Value> table;

    public TransactionStore() {
        this.writeBatch = new WriteBatch();
        this.table = new ConcurrentSkipListMap<>(ByteUtil.BYTE_ORDER_COMPARATOR);
    }

    public WriteBatch getWriteBatch() {
        return writeBatch;
    }

    public Value get(byte[] key) {
        return table.get(key);
    }

    public void put(byte[] key, byte[] value) {
        Value put = Value.Put(value);
        table.put(key, put);
        writeBatch.put(key,put);
    }

    public void delete(byte[] key) {
        final Value delete = Value.Delete();
        table.put(key, delete);
        writeBatch.put(key,delete);
    }
}
