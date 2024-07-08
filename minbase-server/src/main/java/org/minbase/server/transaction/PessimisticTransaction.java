package org.minbase.server.transaction;



import org.minbase.server.transaction.lock.LockHolder;
import org.minbase.server.transaction.lock.PessimisticKeyLock;
import org.minbase.server.transaction.lock.PessimisticLockManager;

import java.util.Map;

public class PessimisticTransaction extends Transaction {
    LockHolder lockHolder;

    public PessimisticTransaction(long transactionId) {
        super(transactionId);
        this.lockHolder = new LockHolder();
        this.keyLock = new PessimisticKeyLock(lockHolder);
    }

    @Override
    protected boolean commitImpl() {
        this.lsmStorage.put(writeBatchTable.getWriteBatch());
        this.transactionState = TransactionState.Commit;
        TransactionManager.getActiveTransactions().remove(this.transactionId);
        releaseLock();
        return true;
    }

    private void releaseLock() {
        for (Map.Entry<byte[], PessimisticLockManager.LockEntry> mapEntry : lockHolder.getHeldLocks().entrySet()) {
            final PessimisticLockManager.LockEntry entry = mapEntry.getValue();
            if (entry.getLockType().equals(PessimisticLockManager.LockType.WRITE)) {
                this.keyLock.unLockWrite(entry.getUserKey());
            } else {
                this.keyLock.unLockRead(entry.getUserKey());
            }
        }
        lockHolder.getHeldLocks().clear();
    }

    @Override
    public void rollback() {
        this.transactionState = TransactionState.Rollback;
        TransactionManager.getActiveTransactions().remove(this.transactionId);
        releaseLock();
    }
}
