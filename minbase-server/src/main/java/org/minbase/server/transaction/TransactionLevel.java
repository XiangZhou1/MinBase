package org.minbase.server.transaction;

public enum TransactionLevel {
    // 读已提交
    READ_COMMIT,
    // 可重复读
    REPEATABLE_READ
}
