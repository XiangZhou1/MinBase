package org.minbase.server.utils;


import org.minbase.common.op.Delete;
import org.minbase.common.op.Put;
import org.minbase.server.constant.Constants;
import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.kv.KeyImpl;
import org.minbase.server.kv.KeyValue;

import java.util.Comparator;
import java.util.Map;

public class KeyValueUtil {
    public static final Comparator<KeyValueIterator> KEY_ITERATOR_COMPARATOR = new Comparator<KeyValueIterator>() {
        @Override
        public int compare(KeyValueIterator o1, KeyValueIterator o2) {
            return o1.key().compareTo(o2.key());
        }
    };


//    public static List<KeyValue> toKeyValue(Put put) {
//        Value value = Value.Put();
//        for (Map.Entry<byte[], byte[]> entry : put.getColumnValues().entrySet()) {
//            value.addColumnValue(entry.getKey(), entry.getValue());
//        }
//        return new KeyValue(new KeyImpl(put.getKey(), Constants.NO_VERSION), value);
//    }
//
//    public static KeyValue toKeyValue(Delete delete) {
//        Value value = new Value();
//        if (delete.getColumns().isEmpty()) {
//            value.setType(Value.TYPE_DELETE_ALL);
//        } else {
//            value.setType(Value.TYPE_DELETE_COLUMN);
//            for (byte[] column : delete.getColumns()) {
//                value.addDeletedColumn(column);
//            }
//
//        }
//        return new KeyValue(new KeyImpl(delete.getKey(), Constants.NO_VERSION), value);
//    }
}
