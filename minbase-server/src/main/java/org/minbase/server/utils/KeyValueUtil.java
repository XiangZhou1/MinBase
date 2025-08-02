package org.minbase.server.utils;


import org.minbase.common.operation.Delete;
import org.minbase.common.operation.Put;
import org.minbase.server.constant.Constants;
import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.table.TableValue;

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
        TableValue tableValue = TableValue.Put();
        for (Map.Entry<byte[], byte[]> entry : put.getColumnValues().entrySet()) {
            tableValue.addColumnValue(entry.getKey(), entry.getValue());
        }
        // todo
        // return new KeyValue(new Key(put.getKey(), Constants.NO_VERSION), tableValue);
        return null;
    }

    public static KeyValue toKeyValue(Delete delete) {
        TableValue tableValue = new TableValue();
        if (delete.getColumns().isEmpty()) {
            tableValue.setType(TableValue.TYPE_DELETE_ALL);
        } else {
            tableValue.setType(TableValue.TYPE_DELETE_COLUMN);
            for (byte[] column : delete.getColumns()) {
                tableValue.addDeletedColumn(column);
            }

        }
        // todo
        //  return new KeyValue(new Key(delete.getKey(), Constants.NO_VERSION), tableValue);
        return null;
    }
}
