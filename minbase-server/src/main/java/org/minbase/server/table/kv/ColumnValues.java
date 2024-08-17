package org.minbase.server.table.kv;

import org.minbase.common.Constants;
import org.minbase.common.utils.ByteUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

public class ColumnValues extends org.minbase.common.op.ColumnValues {

    public ColumnValues() {
    }


    /**
     * |columnLen|column|valueLen|value|
     *
     * @return
     */
    public byte[] encode() {
        byte[] buf = new byte[length()];
        int offset = 0;
        System.arraycopy(ByteUtil.intToByteArray(columnValues.size()), 0, buf, offset, Constants.INTEGER_LENGTH);
        offset += Constants.INTEGER_LENGTH;

        for (Map.Entry<byte[], byte[]> entry : columnValues.entrySet()) {
            byte[] column = entry.getKey();
            byte[] value = entry.getValue();

            System.arraycopy(ByteUtil.intToByteArray(column.length), 0, buf, offset, Constants.INTEGER_LENGTH);
            offset += Constants.INTEGER_LENGTH;
            System.arraycopy(column, 0, buf, offset, column.length);
            offset += column.length;

            System.arraycopy(ByteUtil.intToByteArray(value.length), 0, buf, offset, Constants.INTEGER_LENGTH);
            offset += Constants.INTEGER_LENGTH;
            System.arraycopy(value, 0, buf, offset, value.length);

        }
        return buf;
    }

    public int encodeToFile(OutputStream outputStream) throws IOException {
        outputStream.write(ByteUtil.intToByteArray(columnValues.size()));
        for (Map.Entry<byte[], byte[]> entry : columnValues.entrySet()) {
            byte[] column = entry.getKey();
            byte[] value = entry.getValue();
            outputStream.write(ByteUtil.intToByteArray(column.length));
            outputStream.write(column);
            outputStream.write(ByteUtil.intToByteArray(value.length));
            outputStream.write(value);
            return length();
        }


        return length();
    }

    public void decode(byte[] buf, int offset) {
        final int size = ByteUtil.byteArrayToInt(buf, offset);
        offset += Constants.INTEGER_LENGTH;

        for (int i = 0; i < size; i++) {
            final int columnLen = ByteUtil.byteArrayToInt(buf, offset);
            offset += Constants.INTEGER_LENGTH;

            byte[] column = new byte[columnLen];
            System.arraycopy(buf, offset, column, 0, columnLen);
            offset += columnLen;

            final int valueLen = ByteUtil.byteArrayToInt(buf, offset);
            offset += Constants.INTEGER_LENGTH;

            byte[] value = new byte[valueLen];
            System.arraycopy(buf, offset, value, 0, valueLen);
            columnValues.put(column, value);
        }
    }

    public int length() {
        int offset = Constants.INTEGER_LENGTH;

        for (Map.Entry<byte[], byte[]> entry : columnValues.entrySet()) {
            byte[] column = entry.getKey();
            byte[] value = entry.getValue();

            offset += Constants.INTEGER_LENGTH;
            offset += column.length;
            offset += Constants.INTEGER_LENGTH;
            offset += value.length;
        }
        return offset;
    }

    public void add(byte[] column) {
        columnValues.put(column, null);
    }

    public void add(byte[] column, byte[] value) {
        columnValues.put(column, value);
    }

    @Override
    public String toString() {
        return "ColumnValues{" +
                "columnValues=" + columnValues +
                '}';
    }
}
