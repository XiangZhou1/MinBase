package org.minbase.server.exception;

public class DeadLockException extends TransactionException {
    public DeadLockException() {
    }

    public DeadLockException(String message) {
        super(message);
    }
}
