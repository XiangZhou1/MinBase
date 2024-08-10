package org.minbase.server.utils;


import org.minbase.common.operation.Delete;
import org.minbase.common.operation.Put;
import org.minbase.server.constant.Constants;
import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.op.Value;

import java.util.Comparator;
import java.util.Map;

public class KeyValueUtil {
    public static final Comparator<KeyValueIterator> KEY_ITERATOR_COMPARATOR = new Comparator<KeyValueIterator>() {
        @Override
        public int compare(KeyValueIterator o1, KeyValueIterator o2) {
            return o1.key().compareTo(o2.key());
        }
    };


    public static KeyValue toKeyValue(Put put) {
        Value value = Value.Put();
        for (Map.Entry<byte[], byte[]> entry : put.getColumnValues().entrySet()) {
            value.addColumnValue(entry.getKey(), entry.getValue());
        }
        return new KeyValue(new Key(put.getKey(), Constants.NO_VERSION), value);
    }

    public static KeyValue toKeyValue(Delete delete) {
        Value value = new Value();
        if (delete.getColumns().isEmpty()) {
            value.setType(Value.TYPE_DELETE_ALL);
        } else {
            value.setType(Value.TYPE_DELETE_COLUMN);
            for (byte[] column : delete.getColumns()) {
                value.addDeletedColumn(column);
            }

        }
        return new KeyValue(new Key(delete.getKey(), Constants.NO_VERSION), value);
    }
}
