package org.minbase.server.transaction.writeBatch;



import org.minbase.server.op.Value;
import org.minbase.server.op.WriteBatch;
import org.minbase.server.utils.ByteUtils;

import java.util.concurrent.ConcurrentSkipListMap;

public class WriteBatchTable {
    WriteBatch writeBatch;
    ConcurrentSkipListMap<byte[], Value> table;

    public WriteBatchTable() {
        this.writeBatch = new WriteBatch();
        this.table = new ConcurrentSkipListMap<>(ByteUtils.BYTE_ORDER_COMPARATOR);
    }

    public WriteBatch getWriteBatch() {
        return writeBatch;
    }

    public Value get(byte[] key) {
        return table.get(key);
    }

    public void put(byte[] key, Value value) {
        table.put(key, value);
        writeBatch.put(key,value);
    }
}
