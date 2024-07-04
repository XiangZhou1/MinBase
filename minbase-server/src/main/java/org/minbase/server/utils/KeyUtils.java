package org.minbase.server.utils;


import org.minbase.server.iterator.KeyIterator;

import java.util.Comparator;

public class KeyUtils {
    public static final Comparator<KeyIterator> KEY_ITERATOR_COMPARATOR = new Comparator<KeyIterator>() {
        @Override
        public int compare(KeyIterator o1, KeyIterator o2) {
            return o1.key().compareTo(o2.key());
        }
    };

}
