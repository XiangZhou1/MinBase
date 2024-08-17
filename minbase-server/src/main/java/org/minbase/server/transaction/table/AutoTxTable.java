package org.minbase.server.transaction.table;

import org.minbase.common.exception.TransactionException;
import org.minbase.common.op.ColumnValues;
import org.minbase.common.op.Delete;
import org.minbase.common.op.Get;
import org.minbase.common.op.Put;
import org.minbase.server.table.Table;
import org.minbase.server.transaction.Transaction;
import org.minbase.server.transaction.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutoTxTable implements org.minbase.common.table.Table {
    private static final Logger log = LoggerFactory.getLogger(AutoTxTable.class);
    private Table rawTable ;
    public AutoTxTable(Table rawTable) {
        this.rawTable = rawTable;
    }

    @Override
    public String name() {
        return rawTable.name();
    }

    @Override
    public ColumnValues get(Get get) {
        get.setSequenceId(TransactionManager.newTransactionId());
        return rawTable.get(get);
    }

    @Override
    public void put(Put put) {
        Transaction transaction = TransactionManager.newTransaction(rawTable);
        try {
            TxTable txTable = new TxTable(rawTable, transaction);
            txTable.put(put);
            transaction.commit();
        } catch (TransactionException e) {
            log.error("Auto Commit error", e);
            transaction.rollback();
        }
    }

    @Override
    public boolean checkAndPut(byte[] checkKey, byte[] column, byte[] checkValue, Put put) {
        Transaction transaction = TransactionManager.newTransaction(rawTable);
        try {
            if (rawTable.checkAndPut(checkKey, column, checkValue, put)) {
                transaction.commit();
                return true;
            } else {
                transaction.rollback();
                return false;
            }
        } catch (TransactionException e) {
            transaction.rollback();
            return false;
        }
    }

    @Override
    public void delete(Delete delete) {
        Transaction transaction = TransactionManager.newTransaction(rawTable);
        try {
            TxTable txTable = new TxTable(rawTable, transaction);
            txTable.delete(delete);
            transaction.commit();
        } catch (TransactionException e) {
            log.error("Auto Commit error", e);
            transaction.rollback();
        }
    }
}
