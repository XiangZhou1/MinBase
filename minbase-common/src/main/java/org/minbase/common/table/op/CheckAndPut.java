package org.minbase.common.table.op;

public class CheckAndPut {
    byte[] key;
    byte[] column;
    byte[] value;
    Put put;

    public CheckAndPut(byte[] key, byte[] column, byte[] value, Put put) {
        this.key = key;
        this.column = column;
        this.value = value;
        this.put = put;
    }

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
        return value;
    }
}
