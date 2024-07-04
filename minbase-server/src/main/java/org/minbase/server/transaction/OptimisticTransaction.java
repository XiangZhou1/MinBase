package org.minbase.server.transaction;



import org.minbase.server.transaction.lock.KeyLock;
import org.minbase.server.transaction.lock.OptimisticKeyLock;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 所有的事务commit都是顺序执行的
 *
 * 乐观锁实现
 * 在commit的时候进行事务冲突检查
 * 要进行冲突检查的事务:
 * 1 在本事务创建时, 还活跃的事务; 在本事务提交之前, 就已经提交
 * 2 在本事务创建时, 还未的事务; 在本事务执行时创建; 在本事务提交之前, 就已经提交
 *
 * 无需进行冲突检查的事务
 * 1 在本事务创建时, 还活跃的事务; 在本事务提交之前, 还未提交
 * 2 在本事务创建时, 还未的事务; 在本事务执行时创建; 在本事务提交之前, 还未提交
 *
 * 3 在本事务创建时, 已经提交的事务
 */
public class OptimisticTransaction extends Transaction {
    KeyLock keyLock = new OptimisticKeyLock();
    List<Transaction> checkTransactions = new ArrayList<>();

    public OptimisticTransaction(long transactionId) {
        super(transactionId);
    }

    public KeyLock getKeyLock() {
        return keyLock;
    }

    public void addCheckTransaction(Transaction transaction){
        checkTransactions.add(transaction);
    }

    @Override
    protected boolean commitImpl() {
        //1 在本事务创建时, 还活跃的事务; 在本事务提交之前, 就已经提交
        for (Transaction checkTransaction : activeTransactions) {
            if (checkTransaction.isCommit() && checkConflict(checkTransaction)) {
                return false;
            }
        }

        // 在本事务创建时, 还未的事务; 在本事务执行时创建; 在本事务提交之前, 就已经提交
        for (Transaction checkTransaction : checkTransactions) {
            if (checkConflict(checkTransaction)) {
                return false;
            }
        }

        // 无冲突, 可以写入到其中
        lsmStorage.put(writeBatchTable.getWriteBatch());

        // 给所有在本事务创建之前已经创建的,且还在活跃的事务, 添加需要进行检查冲突的事务(自身)
        final Iterator<Map.Entry<Long, Transaction>> iterator = TransactionManager.getActiveTransactions().entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, Transaction> entry = iterator.next();
            long transactionId = entry.getKey();
            Transaction prevActiveTransaction = entry.getValue();
            if (transactionId < this.transactionId) {
                prevActiveTransaction.addCheckTransaction(this);
            }
        }

        this.transactionState = TransactionState.Commit;
        TransactionManager.getActiveTransactions().remove(this.transactionId);

        return true;
    }

    private boolean checkConflict(Transaction transaction) {
        final OptimisticKeyLock keyLock2 = (OptimisticKeyLock) ((OptimisticTransaction) transaction).getKeyLock();
        return ((OptimisticKeyLock) this.keyLock).checkConflict(keyLock2);
    }
    

    @Override
    public void rollback() {
        this.transactionState = TransactionState.Rollback;
        TransactionManager.getActiveTransactions().remove(this.transactionId);
    }

}
