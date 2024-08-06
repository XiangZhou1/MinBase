package org.minbase.common.operation;

import java.util.ArrayList;
import java.util.List;

public class Get {
    private byte[] key;
    private List<byte[]> columns = new ArrayList<>();

    public Get() {
    }

    public Get(byte[] key) {
        this.key = key;
    }

    public Get(byte[] key, List<byte[]> columns) {
        this.key = key;
        this.columns = columns;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getKey() {
        return key;
    }

    public void addColumn(byte[] column) {
        columns.add(column);
    }

    public List<byte[]> getColumns() {
        return columns;
    }
}
