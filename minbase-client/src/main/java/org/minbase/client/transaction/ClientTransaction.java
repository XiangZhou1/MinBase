package org.minbase.client.transaction;

import org.minbase.common.exception.TransactionException;
import org.minbase.common.table.Table;
import org.minbase.common.transaction.Transaction;

public class ClientTransaction implements Transaction {
    private long txId;

    @Override
    public long txId() {
        return txId;
    }

    @Override
    public void commit() throws TransactionException {

    }

    @Override
    public void rollback() {

    }

    @Override
    public Table getTable(String tableName) {
        return null;
    }
}
