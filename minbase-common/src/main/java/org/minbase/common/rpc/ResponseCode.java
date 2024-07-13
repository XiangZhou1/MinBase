package org.minbase.common.rpc;

public enum ResponseCode {
    SUCCESS((byte)1),
    FAIL((byte)-1);

    private byte code;
    ResponseCode(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }
}
