package org.minbase.common.rpc.service;

public enum CallType {
    CLIENT_GET(1),
    CLIENT_PUT(2),
    CLIENT_CHECK_AND_PUT(3),
    CLIENT_DELETE(4),
    CLIENT_BEGIN_TRANSACTION(5),
    CLIENT_COMMIT(6),
    CLIENT_ROLLBACK(7),

    TX_GET(8),
    TX_PUT(9),
    TX_CHECK_AND_PUT(10),
    TX_DELETE(11),

    ADMIN_CREATE_TABLE(12),
    ADMIN_DROP_TABLE(13),
    ADMIN_TRUNCATE_TABLE(14);

    private final int type;

    CallType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
}
