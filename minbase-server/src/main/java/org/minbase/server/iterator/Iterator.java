package org.minbase.server.iterator;

public interface Iterator <K, V>{
    V value();

    /// Get the current key.
    K key();

    // 锁定到以key开头的[key, ....)的范围
    void seek(K key);

    /// Check if the current iterator is valid.
    boolean isValid();

    /// Move to the next.
    void next();
}

