package org.minbase.common.exception;

import java.io.IOException;

public class TableNotExistException extends IOException {
    public TableNotExistException(String message) {
        super(message);
    }
}
