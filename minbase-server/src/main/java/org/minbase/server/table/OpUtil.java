package org.minbase.server.table;

import org.minbase.common.table.op.Delete;
import org.minbase.common.table.op.Put;
import org.minbase.server.kv.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OpUtil {
    public static List<KeyValue> fromPut(Put put) {
        List<KeyValue> keyValues = new ArrayList<>();
        byte[] key = put.getKey();
        for (Map.Entry<byte[], byte[]> entry : put.getColumnValues().entrySet()) {
            TableKey tableKey = new TableKey(key, entry.getKey());
            Key key1 = new Key(tableKey.encode(), Long.MAX_VALUE);
            Value value = new Value(Op.PUT, entry.getValue());
            keyValues.add(new KeyValue(key1, value));
        }
        return keyValues;
    }

    public static List<KeyValue> fromDelete(Delete delete) {
        List<KeyValue> keyValues = new ArrayList<>();
        byte[] key = delete.getKey();
        for (byte[] column : delete.getColumns()) {
            TableKey tableKey = new TableKey(key, column);
            Key key1 = new Key(tableKey.encode(), Long.MAX_VALUE);
            Value value = new Value(Op.DELETE, null);
            keyValues.add(new KeyValue(key1, value));
        }
        return keyValues;
    }
}
