package org.minbase.common.operation;

import java.util.TreeMap;

public class Put {
    private byte[] key;
    private ColumnValues columnValues = new ColumnValues();

    public Put(byte[] key, byte[] column, byte[] value) {
        this.key = key;
        columnValues.set(column, value);
    }

    public Put(byte[] key) {
        this.key = key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getKey() {
        return key;
    }

    public void addValue(byte[] column, byte[] value) {
        columnValues.set(column, value);
    }

    public TreeMap<byte[], byte[]> getColumnValues() {
        return columnValues.getColumnValues();
    }
}
