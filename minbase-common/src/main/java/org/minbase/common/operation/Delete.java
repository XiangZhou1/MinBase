package org.minbase.common.operation;

import java.util.ArrayList;
import java.util.List;

public class Delete {
    private byte[] key;
    private List<byte[]> columns = new ArrayList<>();

    public Delete() {
    }

    public Delete(byte[] key) {
        this.key = key;
    }

    public Delete(byte[] key, List<byte[]> columnValues) {
        this.key = key;
        this.columns = columnValues;
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
