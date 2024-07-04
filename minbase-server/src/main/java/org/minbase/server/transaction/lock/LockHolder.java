package org.minbase.server.transaction.lock;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

public class LockHolder {
    protected Map<byte[], PessimisticKeyLock.LockEntry> heldLocks = new HashMap<>();
    protected Map<byte[], PessimisticKeyLock.LockEntry> waitingLocks = new HashMap<>();

    protected Thread currentThread;

    public void wakeUp() {
        LockSupport.unpark(currentThread);
    }

    public Map<byte[], PessimisticKeyLock.LockEntry> getHeldLocks() {
        return heldLocks;
    }

    public Map<byte[], PessimisticKeyLock.LockEntry> getWaitingLocks() {
        return waitingLocks;
    }

    public Thread getCurrentThread() {
        return currentThread;
    }


    public void setHeldLocks(Map<byte[], PessimisticKeyLock.LockEntry> heldLocks) {
        this.heldLocks = heldLocks;
    }

    public void setWaitingLocks(Map<byte[], PessimisticKeyLock.LockEntry> waitingLocks) {
        this.waitingLocks = waitingLocks;
    }

    public void setCurrentThread(Thread currentThread) {
        this.currentThread = currentThread;
    }
}
