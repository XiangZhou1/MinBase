package org.minbase.server.transaction.lock;

public class PessimisticKeyLock implements KeyLock{
    private LockHolder lockHolder;

    public PessimisticKeyLock(LockHolder lockHolder) {
        this.lockHolder = lockHolder;
    }

    @Override
    public void readLock(byte[] userKey) {
        PessimisticLockManager.readLock(userKey, lockHolder);
    }

    @Override
    public void writeLock(byte[] userKey) {
        PessimisticLockManager.writeLock(userKey, lockHolder);
    }

    @Override
    public void unLockWrite(byte[] userKey) {
        PessimisticLockManager.unLockWrite(userKey, lockHolder);
    }

    @Override
    public void unLockRead(byte[] userKey) {
        PessimisticLockManager.unLockRead(userKey, lockHolder);
    }
}
