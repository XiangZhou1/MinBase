package org.minbase.server.op;

import org.minbase.common.Constants;
import org.minbase.common.utils.ByteUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Value {
    // 0 delete
    // 1 put
    public static byte TYPE_DELETE_COLUMN = 0;
    public static byte TYPE_DELETE_ALL = 1;
    public static byte TYPE_PUT = 2;

    // put操作还是 delete 操作
    private byte type;
    private ColumnValues columnValues;
    private Set<byte[]> deletedColumns;

    private static Value DELETE = new Value(TYPE_DELETE_ALL);

    public Value() {
    }

    public Value(byte type) {
        this.type = type;
    }

    private Value(byte type, ColumnValues columnValues) {
        this.type = type;
        this.columnValues = columnValues;
    }

    private Value(byte type, Set<byte[]> deletedColumns) {
        this.type = type;
        this.deletedColumns = deletedColumns;
    }

    public static Value Put() {
        return new Value(TYPE_PUT, new ColumnValues());
    }

    public static Value Put(ColumnValues columnValues) {
        return new Value(TYPE_PUT, columnValues);
    }

    public static Value Put(byte[] column, byte[] bytes) {
        ColumnValues columnValues = new ColumnValues();
        columnValues.add(column, bytes);
        return new Value(TYPE_PUT, columnValues);
    }


    public static Value Delete() {
        return DELETE;
    }

    public static Value DeleteColumn() {
        return new Value(TYPE_DELETE_COLUMN, new HashSet<>());
    }

    public static Value DeleteColumn(byte[]... columns) {
        Set<byte[]> deletedColumns = new HashSet<>(Arrays.asList(columns));
        return new Value(TYPE_DELETE_COLUMN, deletedColumns);
    }


    public void addColumnValue(byte[] column, byte[] value) {
        if (type != TYPE_PUT) {
            throw new RuntimeException("Invalid usage");
        }
        this.columnValues.add(column, value);
    }

    public void addDeletedColumn(byte[] column) {
        if (type != TYPE_DELETE_COLUMN) {
            throw new RuntimeException("Invalid usage");
        }
        this.deletedColumns.add(column);
    }

    public byte[] encode() {
        byte[] buf = new byte[length()];
        buf[0] = type;
        if (type == TYPE_DELETE_ALL) {
            return buf;
        } else if (type == TYPE_PUT) {
            byte[] encodeValue = columnValues.encode();
            System.arraycopy(encodeValue, 0, buf, 1, encodeValue.length);
        } else {
            int offset = 1;
            System.arraycopy(ByteUtil.intToByteArray(deletedColumns.size()), 0, buf, offset, Constants.INTEGER_LENGTH);
            offset += Constants.INTEGER_LENGTH;

            for (byte[] deletedColumn : deletedColumns) {
                System.arraycopy(ByteUtil.intToByteArray(deletedColumn.length), 0, buf, offset, Constants.INTEGER_LENGTH);
                offset += Constants.INTEGER_LENGTH;

                System.arraycopy(deletedColumn, 0, buf, offset, deletedColumn.length);
                offset += deletedColumn.length;
            }
        }
        return buf;
    }

    // | type(1)|size(4)|column|column|
    public int encodeToFile(OutputStream outputStream) throws IOException {
        outputStream.write(type);
        if (type == TYPE_DELETE_ALL) {
            return 1;
        } else if (type == TYPE_PUT) {
            columnValues.encodeToFile(outputStream);
        } else {
            outputStream.write(ByteUtil.intToByteArray(deletedColumns.size()));
            for (byte[] deletedColumn : deletedColumns) {
                outputStream.write(ByteUtil.intToByteArray(deletedColumn.length));
                outputStream.write(deletedColumn);
            }
        }

        return length();
    }

    public void decode(byte[] buf) {
        type = buf[1];
        if (type == TYPE_DELETE_ALL) {
            return;
        } else if (type == TYPE_PUT) {
            columnValues = new ColumnValues();
            columnValues.decode(buf, 1);
        } else {
            deletedColumns = new HashSet<>();
            int offset = 1;
            int size = ByteUtil.byteArrayToInt(buf, offset);
            offset += Constants.INTEGER_LENGTH;
            for (int i = 0; i < size; i++) {
                int length = ByteUtil.byteArrayToInt(buf, offset);

                offset += Constants.INTEGER_LENGTH;

                byte[] column = new byte[length];
                System.arraycopy(buf, offset, column, 0, length);
                offset += length;
                deletedColumns.add(column);
            }

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

    public Set<byte[]> getDeletedColumns() {
        return deletedColumns;
    }

    public void setType(byte type) {
        this.type = type;
    }
}
