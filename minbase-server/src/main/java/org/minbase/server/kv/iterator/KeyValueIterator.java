package org.minbase.server.kv.iterator;


import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;

public interface KeyValueIterator {
    KeyValue value();

    /// Get the current key.
    Key key();

    // 锁定到以key开头的[key, ....)的范围
    void seek(Key key);

    /// Check if the current iterator is valid.
    boolean hasNext();

    KeyValue next();
   
    default void close() {
    }
}
