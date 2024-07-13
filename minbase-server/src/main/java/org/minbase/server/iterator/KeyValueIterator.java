package org.minbase.server.iterator;


import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;

public interface KeyValueIterator extends Iterator<Key, KeyValue> {
    KeyValue value();

    /// Get the current key.
    Key key();

    // 锁定到以key开头的[key, ....)的范围
    void seek(Key key);

    /// Check if the current iterator is valid.
    boolean isValid();

    // 内部的一个迭代器
    void nextInnerKey();
   
    default void close() {

    }
}
