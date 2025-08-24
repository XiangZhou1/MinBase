package org.minbase.common.table.transaction;

import org.minbase.common.exception.TransactionException;
import org.minbase.common.table.ClientTable;
import org.minbase.common.table.TxTable;

import java.io.IOException;

public interface Transaction {
    long txId();

    void commit() throws TransactionException, IOException;

    void rollback() throws IOException;

    TxTable getTable(String tableName);
}
