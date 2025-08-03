package org.minbase.server.kv;

/**
 * KV操作类型
 */
public enum Op {
    // put
    PUT((byte) 1),
    // delete
    DELETE((byte) 2);
    private byte op;

    Op(byte op) {
        this.op = op;
    }

    public byte getOp() {
        return op;
    }
}
