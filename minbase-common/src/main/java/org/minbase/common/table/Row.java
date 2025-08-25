package org.minbase.common.table;

import org.minbase.common.table.op.ColumnValues;
import org.minbase.common.utils.ByteUtil;

public class Row {
    String rowKey;
    ColumnValues columnValues = new ColumnValues();

    public Row() {
    }

    public Row(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public ColumnValues getColumnValues() {
        return columnValues;
    }

    public void add(String column, String value) {
        columnValues.set(ByteUtil.toBytes(column), ByteUtil.toBytes(value));
    }
    public void add(byte[] column, byte[]  value) {
        columnValues.set(column, value);
    }
}
