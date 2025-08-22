package org.minbase.server.table;

import org.minbase.common.utils.ByteUtil;
import org.minbase.server.constant.Constants;
import org.minbase.server.kv.Length;
import org.minbase.server.kv.utils.Codec;

public class TableKey implements Length, Codec {
    private byte[] key;
    private byte[] column;
    // |keyLength (int)| key | column |
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
        System.arraycopy(ByteUtil.intToByteArray(key.length), 0, bytes, pos, Constants.INTEGER_LENGTH);
        pos += Constants.INTEGER_LENGTH;
        System.arraycopy(key, 0, bytes, pos, key.length);
        pos += key.length;
        System.arraycopy(column, 0, bytes, pos, column.length);
        return bytes;
    }

    @Override
    public void decode(byte[] val) {
        int pos = 0;
        int keyLength = ByteUtil.byteArrayToInt(val, 0);
        pos += Constants.INTEGER_LENGTH;
        key = new byte[keyLength];
        System.arraycopy(val, pos, key, 0, keyLength);
        pos += keyLength;
        column = new byte[val.length - pos];
        System.arraycopy(val, pos, column, 0, column.length);
    }

    public byte[] getColumn() {
        return column;
    }
}
