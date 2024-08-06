package org.minbase.server.exception;

/**
 * 乐观锁发生冲突
 */
public class OptimisticConflictException extends RuntimeException {
    public OptimisticConflictException() {
    }

    public OptimisticConflictException(String message) {
        super(message);
    }
}
