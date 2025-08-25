package org.minbase.server.table;

import org.minbase.common.rpc.Constant;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.constant.Constants;
import org.minbase.server.kv.Length;
import org.minbase.server.kv.utils.Codec;

public class TableKey implements Length, Codec {
    private byte[] key;
    private byte[] column;
    // | key | column | keyLength (int)|
    private int length;

    public TableKey() {
    }

    public TableKey(byte[] key, byte[] column) {
        this.key = key;
        this.column = column;
        this.length = key.length + column.length + Constants.INTEGER_LENGTH;
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public byte[] encode() {
        byte[] bytes = new byte[length];
        int pos = 0;
        System.arraycopy(key, 0, bytes, pos, key.length);
        pos += key.length;
        System.arraycopy(column, 0, bytes, pos, column.length);
        pos += column.length;
        System.arraycopy(ByteUtil.intToByteArray(key.length), 0, bytes, pos, Constants.INTEGER_LENGTH);
        return bytes;
    }

    @Override
    public void decode(byte[] val) {
        length = val.length;
        int keyLength = ByteUtil.byteArrayToInt(val, val.length - Constants.INTEGER_LENGTH);

        int pos = 0;
        key = new byte[keyLength];
        System.arraycopy(val, pos, key, 0, keyLength);
        pos += keyLength;
        column = new byte[val.length - pos - Constants.INTEGER_LENGTH];
        System.arraycopy(val, pos, column, 0, column.length);
    }

    public byte[] getColumn() {
        return column;
    }

    public byte[] getKey() {
        return key;
    }
}
