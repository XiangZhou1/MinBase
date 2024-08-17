package org.minbase.server.table.kv;

import org.minbase.common.Constants;
import org.minbase.common.utils.ByteUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

public class ColumnValue {
    private byte[] column;
    private byte[] value;

    public ColumnValue() {
    }


    public ColumnValue(byte[] column, byte[] value) {
        this.column = column;
        this.value = value;
    }

    /**
     * |columnLen|column|valueLen|value|
     *
     * @return
     */
    public byte[] encode() {
        byte[] buf = new byte[length()];
        int offset = 0;

        System.arraycopy(ByteUtil.intToByteArray(column.length), 0, buf, offset, Constants.INTEGER_LENGTH);
        offset += Constants.INTEGER_LENGTH;
        System.arraycopy(column, 0, buf, offset, column.length);
        offset += column.length;

        System.arraycopy(ByteUtil.intToByteArray(value.length), 0, buf, offset, Constants.INTEGER_LENGTH);
        offset += Constants.INTEGER_LENGTH;
        System.arraycopy(value, 0, buf, offset, value.length);

        return buf;
    }

    public int encodeToFile(OutputStream outputStream) throws IOException {
        outputStream.write(ByteUtil.intToByteArray(column.length));
        outputStream.write(column);
        outputStream.write(ByteUtil.intToByteArray(value.length));
        outputStream.write(value);

        return length();
    }

    public void decode(byte[] buf, int offset) {
        final int columnLen = ByteUtil.byteArrayToInt(buf, offset);
        offset += Constants.INTEGER_LENGTH;

        column = new byte[columnLen];
        System.arraycopy(buf, offset, column, 0, columnLen);
        offset += Constants.INTEGER_LENGTH;

        final int valueLen = ByteUtil.byteArrayToInt(buf, offset);
        offset += Constants.INTEGER_LENGTH;

        value = new byte[valueLen];
        System.arraycopy(buf, offset, value, 0, valueLen);
    }

    public byte[] value() {
        return value;
    }

    public byte[] column() {
        return column;
    }

    public int length() {
        return 2 * Constants.INTEGER_LENGTH + column.length + value.length;
    }

    @Override
    public String toString() {
        return "ColumnValue{" +
                "column=" + Arrays.toString(column) +
                ", value=" + Arrays.toString(value) +
                '}';
    }
}
