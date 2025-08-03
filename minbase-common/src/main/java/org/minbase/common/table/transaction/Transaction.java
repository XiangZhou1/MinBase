package org.minbase.common.table.transaction;

import org.minbase.common.exception.TransactionException;
import org.minbase.common.table.Table;

public interface Transaction {
    long txId();

    void commit() throws TransactionException;

    void rollback();

    Table getTable(String tableName);
}
