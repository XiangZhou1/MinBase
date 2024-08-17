package org.minbase.common.op;

import org.minbase.common.utils.ByteUtil;

import java.util.TreeMap;

public class ColumnValues {
    // column, value
    protected TreeMap<byte[], byte[]> columnValues = new TreeMap(ByteUtil.BYTE_ORDER_COMPARATOR);

    public byte[] get(byte[] column) {
        return columnValues.get(column);
    }

    public void set(byte[] column, byte[] value) {
        columnValues.put(column, value);
    }

    public TreeMap<byte[], byte[]> getColumnValues() {
        return columnValues;
    }

    public int size() {
        return columnValues.size();
    }
}
