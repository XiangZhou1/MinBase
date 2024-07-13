package org.minbase.server.utils;


import org.minbase.server.iterator.KeyValueIterator;

import java.util.Comparator;

public class KeyUtil {
    public static final Comparator<KeyValueIterator> KEY_ITERATOR_COMPARATOR = new Comparator<KeyValueIterator>() {
        @Override
        public int compare(KeyValueIterator o1, KeyValueIterator o2) {
            return o1.key().compareTo(o2.key());
        }
    };

}
