package org.minbase.common.exception;

import java.io.IOException;

public class TransactionNotExistException extends IOException {
    public TransactionNotExistException() {
    }

    public TransactionNotExistException(String message) {
        super(message);
    }
}
