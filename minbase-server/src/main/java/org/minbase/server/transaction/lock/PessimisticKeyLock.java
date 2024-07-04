package org.minbase.server.transaction.lock;



import org.minbase.server.exception.DeadLockException;
import org.minbase.server.utils.ByteUtils;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.LockSupport;

public class PessimisticKeyLock implements KeyLock {

    @Override
    public void readLock(byte[] userKey) {

    }

    @Override
    public void writeLock(byte[] userKey) {

    }

    public enum LockType {
        READ,
        WRITE,
    }

    // 锁的信息
    public static class LockEntry {
        byte[] userKey;
        LockType lockType;
        Set<LockHolder> lockHolders = new HashSet<>();
        List<LockHolder> waitHolders = new ArrayList<>();

        public LockEntry(byte[] userKey, LockType lockType, LockHolder lockHolder) {
            this.userKey = userKey;
            this.lockHolders.add(lockHolder);
            this.lockType = lockType;
        }

        public byte[] getUserKey() {
            return userKey;
        }

        public LockType getLockType() {
            return lockType;
        }

        public Set<LockHolder> getLockHolders() {
            return lockHolders;
        }

        public List<LockHolder> getWaitHolders() {
            return waitHolders;
        }
    }

    ConcurrentSkipListMap<byte[], LockEntry> locks = new ConcurrentSkipListMap<>();

    private PessimisticKeyLock() {
    }

    public static PessimisticKeyLock getInstance() {
        return new PessimisticKeyLock();
    }

    public synchronized void readLock(byte[] userKey, LockHolder lockHolder) {
        lockHolder.setCurrentThread(Thread.currentThread());
        while (true) {
            LockEntry entry = new LockEntry(userKey, LockType.READ, lockHolder);
            LockEntry lockEntry = locks.putIfAbsent(userKey, entry);
            // 第一次加锁
            if (lockEntry == null) {
                lock(lockHolder, entry);
                return;
            }
            // 当前存在的锁是读锁
            if (lockEntry.lockType.equals(LockType.READ)) {
                lock(lockHolder, lockEntry);
            } else {
                // 当前存在的锁是写锁, 锁的持有者是本身
                if (lockEntry.lockHolders.contains(lockHolder)) {
                    return;
                }
            }
            // 等待其他holder释放锁
            waitingLock(lockHolder, lockEntry);
            if (checkDeadLock(userKey, lockHolder.heldLocks, false)) {
                lockEntry.waitHolders.remove(lockHolder);
                throw new DeadLockException("");
            }
            LockSupport.park();
        }
    }

    private void waitingLock(LockHolder lockHolder, LockEntry lockEntry) {
        lockEntry.waitHolders.add(lockHolder);
        lockHolder.waitingLocks.put(lockEntry.userKey, lockEntry);
    }

    private void lock(LockHolder lockHolder, LockEntry entry) {
        entry.lockHolders.add(lockHolder);
        lockHolder.heldLocks.put(entry.userKey, entry);
        entry.waitHolders.remove(lockHolder);
        lockHolder.waitingLocks.remove(entry.userKey);
    }

    public synchronized void writeLock(byte[] userKey, LockHolder lockHolder) {
        lockHolder.setCurrentThread(Thread.currentThread());
        while (true) {
            LockEntry entry = new LockEntry(userKey, LockType.WRITE, lockHolder);
            LockEntry lockEntry = locks.putIfAbsent(userKey, entry);
            // 第一次加锁
            if (lockEntry == null) {
                lock(lockHolder, entry);
                return;
            }
            // 本身
            if (lockEntry.lockType.equals(LockType.WRITE) && lockEntry.lockHolders.contains(lockHolder)) {
                return;
            }
            // 本身是读锁, 升级为写锁
            if (lockEntry.lockType.equals(LockType.READ) && lockEntry.lockHolders.contains(lockHolder)  && lockEntry.lockHolders.size()==1) {
                lockEntry.lockType = LockType.WRITE;
                return;
            }
            waitingLock(lockHolder, lockEntry);
            if (checkDeadLock(userKey,  lockHolder.heldLocks, true)) {
                lockEntry.waitHolders.remove(lockHolder);
                throw new DeadLockException("");
            }
            LockSupport.park();
        }
    }

    public void unLockWrite(byte[] userKey) {
        LockEntry lockEntry = locks.remove(userKey);
        for (LockHolder waitHolder : lockEntry.waitHolders) {
            waitHolder.wakeUp();
        }
    }

    public void unLockRead(byte[] userKey, LockHolder lockHolder) {
        LockEntry lockEntry = locks.get(userKey);
        if (lockEntry.lockHolders.size() == 1) {
            locks.remove(userKey);
            for (LockHolder waitHolder : lockEntry.waitHolders) {
                waitHolder.wakeUp();
            }
        }
    }

    /**
     * 死锁检测
     * lockHolder1 已经获得lock1,  希望获得lock2, lock2.waitHolders.add(lockHolder1)
     * lockHolder2 已经获得lock2,  希望获得lock1, lock1.waitHolders.add(lockHolder2)
     * 成环了,  就是死锁
     *
     *
     * 分析哪些情况会死锁:
     * 情况1: 不会死锁
     * lockHolder1 已经获得key1读锁,  希望获得key2读锁
     * lockHolder1 已经获得key2读锁,  希望获得key1读锁
     *
     * 情况2: 不会死锁
     * lockHolder1 已经获得key1读锁,  希望获得key2写锁
     * lockHolder2 已经获得key2读锁,  希望获得key1读锁
     *
     * 情况3: 会死锁
     * lockHolder1 已经获得key1读锁,  希望获得key2写锁
     * lockHolder1 已经获得key2读锁,  希望获得key1写锁
     *
     * 情况4: 会死锁
     * lockHolder1 已经获得key1写锁,  希望获得key2写锁
     * lockHolder1 已经获得key2读锁,  希望获得key1写锁
     *
     * 当形成一个环时,
     * 已经获得与希望获得的锁类型
     * 读写， 写写都会死锁
     * 读读 不会死锁
     *
     * 从哪里计算一个
     *
     *
     * a -> b, b->c , c->a
     */

    /**
     * lockHolder希望获得uerKey的锁,
     * lockHolder 得去查看自己已经获得锁里面，有谁在等待
     * @param userKey   lockHolder希望获得uerKey的锁
     * @param heldLocks lockHolder已经持有的锁
     * @return
     */
    private boolean checkDeadLock(byte[] userKey, Map<byte[], LockEntry> heldLocks, boolean wantWriteLock) {
        for (Map.Entry<byte[], LockEntry> entry : heldLocks.entrySet()) {
            LockEntry heldLock = entry.getValue();
            if (ByteUtils.byteEqual(userKey, entry.getKey())) {
                if(heldLock.lockType.equals(LockType.WRITE) || wantWriteLock)
                return true;
            }
            // 查看已经获得锁里面，有谁在等待; 等待者都已经获得哪些锁, 这些等待者已经获得的写锁是否是userKey
            for (LockHolder waitHolder : heldLock.waitHolders) {
                checkDeadLock(userKey, waitHolder.heldLocks, wantWriteLock);
            }
        }
        return false;
    }

}
