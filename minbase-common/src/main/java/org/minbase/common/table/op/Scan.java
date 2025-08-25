package org.minbase.common.table.op;

public class Scan {
    byte[] startKey;
    byte[] endKey;
    int rowCountLimit = 10000;

    public Scan() {
    }

    public Scan(byte[] startKey, byte[] endKey) {
        this.startKey = startKey;
        this.endKey = endKey;
    }

    public Scan(byte[] startKey, byte[] endKey, int rowCountLimit) {
        this.startKey = startKey;
        this.endKey = endKey;
        this.rowCountLimit = rowCountLimit;
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public void setStartKey(byte[] startKey) {
        this.startKey = startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    public void setEndKey(byte[] endKey) {
        this.endKey = endKey;
    }

    public int getRowCountLimit() {
        return rowCountLimit;
    }

    public void setRowCountLimit(int rowCountLimit) {
        this.rowCountLimit = rowCountLimit;
    }
}
