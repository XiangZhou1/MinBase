package org.minbase.common.rpc.service;

public enum StatusCode {
    SUCCESS(1),
    FAIL(2),
    ERROR_TABLE_NOT_EXIST(3),
    ERROR_TRANSACTION_CONFLICT(4),
    ERROR_IO_ERRER(5),
    ERROR_DEFAULT(6),
    ERROR_TRANSACTION_NOT_EXIST(7);

    int code;

    StatusCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
