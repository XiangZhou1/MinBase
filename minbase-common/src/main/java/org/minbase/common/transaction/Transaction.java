package org.minbase.common.transaction;

import org.minbase.common.exception.TransactionException;

public interface Transaction {
    void commit() throws TransactionException;

    void rollback();
}
