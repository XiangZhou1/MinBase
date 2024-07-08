package org.minbase.server.transaction.lock;

import org.minbase.server.utils.ByteUtils;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public class OptimisticKeyLock implements KeyLock {
    Set<byte[]> readLockSet = new ConcurrentSkipListSet<>(ByteUtils.BYTE_ORDER_COMPARATOR);
    Set<byte[]> writeLockSet = new ConcurrentSkipListSet<>(ByteUtils.BYTE_ORDER_COMPARATOR);

    @Override
    public void readLock(byte[] userKey) {
        readLockSet.add(userKey);
    }

    @Override
    public void writeLock(byte[] userKey) {
        writeLockSet.add(userKey);
    }


    private boolean readConflict(byte[] userKey) {
        return writeLockSet.contains(userKey);
    }

    private boolean writeConflict(byte[] userKey) {
        return writeLockSet.contains(userKey) || readLockSet.contains(userKey);
    }

    /**
     * 检查是否有冲突
     */
    public boolean checkConflict(OptimisticKeyLock keyLock2) {
        for (byte[] userKey : readLockSet) {
            if (keyLock2.readConflict(userKey)) {
                return true;
            }
        }

        for (byte[] userKey : writeLockSet) {
            if (keyLock2.writeConflict(userKey)) {
                return true;
            }
        }
        return false;
    }


    @Override
    public void unLockWrite(byte[] userKey) {

    }

    @Override
    public void unLockRead(byte[] userKey) {

    }
}
