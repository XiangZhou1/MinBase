package org.minbase.common.table.op;

public class CheckAndPut {
    byte[] key;
    byte[] column;
    Put put;

    public byte[] getKey() {
        return key;
    }

    public byte[] getColumn() {
        return column;
    }

    public Put getPut() {
        return put;
    }

    public byte[] getValue() {
        return null;
    }
}
