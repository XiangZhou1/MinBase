package org.minbase.server.op;

import org.minbase.common.Constants;
import org.minbase.server.op.ColumnValues;
import org.minbase.common.utils.ByteUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

public class Value {
    // 0 delete
    // 1 put
    public static byte TYPE_DELETE_COLUMN = 0;
    public static byte TYPE_DELETE_ALL = 1;
    public static byte TYPE_PUT = 2;

    // put操作还是
    private byte type;
    private ColumnValues columnValues;

    private static Value DELETE = new Value(TYPE_DELETE_ALL, null);

    public Value() {
        this.columnValues = new ColumnValues();
    }

    public Value(byte type, ColumnValues columnValues) {
        this.type = type;
        this.columnValues = columnValues;
    }

    public static Value Put() {
        return new Value(TYPE_PUT, new ColumnValues());
    }

    public static Value Put(ColumnValues columnValues) {
        return new Value(TYPE_PUT, columnValues);
    }

    public static Value Delete() {
        return DELETE;
    }

    public static Value DeleteColumn() {
        return new Value(TYPE_DELETE_COLUMN, new ColumnValues());
    }

    public static Value DeleteColumn(byte[]... columns) {
        final ColumnValues columnValues = new ColumnValues();

        for (byte[] column : columns) {
            columnValues.add(column);
        }
        return new Value(TYPE_DELETE_COLUMN, columnValues);
    }

    public void addColumnValue(byte[] column, byte[] value) {
        this.columnValues.add(column, value);
    }


    public byte[] encode() {
        byte[] buf = new byte[length()];
        buf[0] = type;
        if (columnValues != null) {
            byte[] encodeValue = columnValues.encode();
            System.arraycopy(encodeValue, 0, buf, 1, encodeValue.length);
        }
        return buf;
    }

    // | type(1)|size(4)|column|column|
    public int encodeToFile(OutputStream outputStream) throws IOException {
        outputStream.write(type);
        if (columnValues != null) {
            columnValues.encodeToFile(outputStream);
        }
        return length();
    }

    public void decode(byte[] buf) {
        type = buf[1];
        if (buf.length != 1) {
            columnValues.decode(buf, 1);
        }
    }

    public byte type() {
        return type;
    }


    public int length() {
        return columnValues.length() + 1;
    }

    public boolean isDelete() {
        return type == TYPE_DELETE_ALL;
    }

    public boolean isDeleteColumn() {
        return type == TYPE_DELETE_COLUMN;
    }

    @Override
    public String toString() {
        return "Value{" +
                "type=" + type +
                ", columnValues=" + columnValues +
                '}';
    }

    public Map<byte[], byte[]> getColumnValues() {
        return columnValues.getColumnValues();
    }

    public ColumnValues columnValues() {
        return columnValues;
    }

    public Set<byte[]> getColumns() {
        return columnValues.getColumnValues().keySet();
    }
}
