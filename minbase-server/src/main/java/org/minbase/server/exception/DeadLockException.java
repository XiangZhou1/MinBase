package org.minbase.server.exception;

public class DeadLockException extends RuntimeException {
    public DeadLockException() {
    }

    public DeadLockException(String message) {
        super(message);
    }
}
