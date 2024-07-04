package org.minbase.server.transaction.lock;

public interface KeyLock {
    void readLock(byte[] userKey);
    void writeLock(byte[] userKey);
}
