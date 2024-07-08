package org.minbase.server.transaction.lock;



import org.minbase.server.exception.DeadLockException;
import org.minbase.server.utils.ByteUtils;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Function;

public class PessimisticLockManager{

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
        ReentrantLock reentrantLock = new ReentrantLock();

        public LockEntry(byte[] userKey, LockType lockType) {
            this.userKey = userKey;
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

        public void lock(){
            this.reentrantLock.lock();
        }
        public void unlock(){
            this.reentrantLock.unlock();
        }
    }

    static ConcurrentSkipListMap<byte[], LockEntry> locks = new ConcurrentSkipListMap<>(ByteUtils.BYTE_ORDER_COMPARATOR);


    public static void readLock(byte[] userKey, LockHolder lockHolder) {
        final LockEntry lockEntry = locks.computeIfAbsent(userKey, new Function<byte[], LockEntry>() {
            @Override
            public LockEntry apply(byte[] bytes) {
                return new LockEntry(userKey, LockType.WRITE);
            }
        });

        synchronized (lockEntry){
            lockHolder.setCurrentThread(Thread.currentThread());
            while (true) {
                // 第一次加锁
                if (lockEntry.lockHolders.isEmpty()) {
                    lock(lockHolder, lockEntry);
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
                try {
                    lockEntry.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private static void waitingLock(LockHolder lockHolder, LockEntry lockEntry) {
        lockEntry.waitHolders.add(lockHolder);
        lockHolder.waitingLocks.put(lockEntry.userKey, lockEntry);
    }

    private static void lock(LockHolder lockHolder, LockEntry entry) {
        entry.lockHolders.add(lockHolder);
        lockHolder.heldLocks.put(entry.userKey, entry);
        entry.waitHolders.remove(lockHolder);
        lockHolder.waitingLocks.remove(entry.userKey);
    }

    public static  void writeLock(byte[] userKey, LockHolder lockHolder) {
        final LockEntry lockEntry = locks.computeIfAbsent(userKey, new Function<byte[], LockEntry>() {
            @Override
            public LockEntry apply(byte[] bytes) {
                return new LockEntry(userKey, LockType.WRITE);
            }
        });

        synchronized (lockEntry) {
            lockHolder.setCurrentThread(Thread.currentThread());
            while (true) {

                // 第一次加锁
                if (lockEntry.lockHolders.isEmpty()) {
                    lock(lockHolder, lockEntry);
                    return;
                }
                // 本身
                if (lockEntry.lockType.equals(LockType.WRITE) && lockEntry.lockHolders.contains(lockHolder)) {
                    return;
                }
                // 本身是读锁, 升级为写锁
                if (lockEntry.lockType.equals(LockType.READ) && lockEntry.lockHolders.contains(lockHolder) && lockEntry.lockHolders.size() == 1) {
                    lockEntry.lockType = LockType.WRITE;
                    return;
                }
                waitingLock(lockHolder, lockEntry);
                if (checkDeadLock(userKey, lockHolder.heldLocks, true)) {
                    lockEntry.waitHolders.remove(lockHolder);
                    throw new DeadLockException("");
                }
                try {
                    lockEntry.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static  void unLockWrite(byte[] userKey, LockHolder lockHolder) {
        final LockEntry entry = locks.get(userKey);
        if (entry == null || !entry.lockHolders.contains(lockHolder)) {
            return;
        }
        synchronized (entry) {
            entry.lockHolders.remove(lockHolder);
            if (entry.waitHolders.isEmpty()) {
                locks.remove(userKey);
            } else {
                entry.notifyAll();
            }
        }
    }

    public static void unLockRead(byte[] userKey, LockHolder lockHolder) {
        final LockEntry entry = locks.get(userKey);
        if (entry == null || !entry.lockHolders.contains(lockHolder)) {
            return;
        }
        synchronized (entry) {
            entry.lockHolders.remove(lockHolder);
            if (entry.waitHolders.isEmpty() && entry.lockHolders.isEmpty()) {
                locks.remove(userKey);
            }

            if (!entry.waitHolders.isEmpty()) {
                entry.notifyAll();
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
    private static boolean checkDeadLock(byte[] userKey, Map<byte[], LockEntry> heldLocks, boolean wantWriteLock) {
        for (Map.Entry<byte[], LockEntry> entry : heldLocks.entrySet()) {
            LockEntry heldLock = entry.getValue();
            if (ByteUtils.byteEqual(userKey, entry.getKey())) {
                if(heldLock.lockType.equals(LockType.WRITE) || wantWriteLock)
                return true;
            }
            // 查看已经获得锁里面，有谁在等待; 等待者都已经获得哪些锁, 这些等待者已经获得的写锁是否是userKey
            for (LockHolder waitHolder : heldLock.waitHolders) {
                if (checkDeadLock(userKey, waitHolder.heldLocks, wantWriteLock)) {
                    return true;
                }
            }
        }
        return false;
    }

}
