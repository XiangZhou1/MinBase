package org.minbase.server.transaction;



import org.minbase.server.transaction.lock.LockHolder;
import org.minbase.server.transaction.lock.PessimisticKeyLock;

import java.util.Map;

public class PessimisticTransaction extends Transaction {
    LockHolder lockHolder;

    public PessimisticTransaction(long transactionId) {
        super(transactionId);
        this.lockHolder = new LockHolder();
        this.keyLock = PessimisticKeyLock.getInstance();
    }

    @Override
    protected boolean commitImpl() {
        this.transactionState = TransactionState.Commit;
        TransactionManager.getActiveTransactions().remove(this.transactionId);
        releaseLock();
        return true;
    }

    private void releaseLock() {
        for (Map.Entry<byte[], PessimisticKeyLock.LockEntry> mapEntry : lockHolder.getHeldLocks().entrySet()) {
            final PessimisticKeyLock.LockEntry entry = mapEntry.getValue();
            if (entry.getLockType().equals(PessimisticKeyLock.LockType.WRITE)) {
                PessimisticKeyLock.getInstance().unLockWrite(entry.getUserKey());
            } else {
                PessimisticKeyLock.getInstance().unLockRead(entry.getUserKey(), lockHolder);
            }
        }
    }

    @Override
    public void rollback() {
        this.transactionState = TransactionState.Rollback;
        TransactionManager.getActiveTransactions().remove(this.transactionId);
        releaseLock();
    }
}
