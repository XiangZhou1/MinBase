package org.minbase.server.kv.utils;


import org.minbase.common.table.op.Delete;
import org.minbase.common.table.op.Put;
import org.minbase.server.kv.iterator.KeyValueIterator;
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
}
