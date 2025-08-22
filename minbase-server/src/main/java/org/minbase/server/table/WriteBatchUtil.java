package org.minbase.server.table;

import org.minbase.common.table.op.Delete;
import org.minbase.common.table.op.Put;
import org.minbase.server.kv.*;

import java.util.Map;

public class WriteBatchUtil {
    public static WriteBatch fromPut(String storeName, Put put) {
        WriteBatch writeBatch = new WriteBatch();
        byte[] key = put.getKey();
        for (Map.Entry<byte[], byte[]> entry : put.getColumnValues().entrySet()) {
            TableKey tableKey = new TableKey(key, entry.getKey());
            Key key1 = new Key(tableKey.encode(), Long.MAX_VALUE);
            Value value = new Value(Op.PUT, entry.getValue());
            writeBatch.add(storeName, new KeyValue(key1, value));
        }
        return writeBatch;
    }

    public static WriteBatch fromDelete(String storeName, Delete delete) {
        WriteBatch writeBatch = new WriteBatch();
        byte[] key = delete.getKey();
        for (byte[] column : delete.getColumns()) {
            TableKey tableKey = new TableKey(key, column);
            Key key1 = new Key(tableKey.encode(), Long.MAX_VALUE);
            Value value = new Value(Op.DELETE, null);
            writeBatch.add(storeName, new KeyValue(key1, value));
        }
        return writeBatch;
    }
}
