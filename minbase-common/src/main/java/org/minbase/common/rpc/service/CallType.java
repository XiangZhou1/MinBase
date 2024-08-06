package org.minbase.common.rpc.service;

public enum CallType {
    CLIENT_GET(1),
    CLIENT_PUT(2),
    CLIENT_CHECK_AND_PUT(2),
    CLIENT_DELETE(2);
    private final int type;

    CallType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
}
