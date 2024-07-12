package org.minbase.server.iterator;


import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;

public interface KeyIterator extends Iterator<Key, KeyValue> {
    KeyValue value();

    /// Get the current key.
    Key key();

    // 锁定到以key开头的[key, ....)的范围
    void seek(Key key);

    /// Check if the current iterator is valid.
    boolean isValid();
    /// Move to the next userKey.
    void nextKey();
    void nextUserKey();
   
    default void close() {

    }

    @Override
    default void next() {

    }
}
