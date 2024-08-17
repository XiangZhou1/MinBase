package org.minbase.server.table.kv;

import org.minbase.common.utils.ByteUtil;
import org.minbase.server.constant.Constants;
import org.minbase.server.kv.Key;

public class InternalKey extends Key {
    private byte[] internalKey;
    private int userKeyLength;

    @Override
    public void setKey(byte[] key) {

    }

    @Override
    public byte[] getKey() {
        return internalKey;
    }

    public InternalKey() {
    }

    public InternalKey(byte[] userKey, byte[] column) {
        this.userKeyLength = userKey.length;
        internalKey = new byte[Constants.INTEGER_LENGTH + userKey.length + column.length];
        System.arraycopy(ByteUtil.intToByteArray(userKey.length), 0, internalKey, 0, Constants.INTEGER_LENGTH);
        System.arraycopy(userKey, 0, internalKey, Constants.INTEGER_LENGTH, userKey.length);
        System.arraycopy(column, 0, internalKey, Constants.INTEGER_LENGTH + userKey.length, column.length);
    }

    public InternalKey(byte[] userKey, byte[] column, long sequenceId) {
        this(userKey, column);
        this.sequenceId = sequenceId;
    }
    public InternalKey(byte[] key, long sequenceId) {
        super(key, sequenceId);
        this.userKeyLength = ByteUtil.byteArrayToInt(internalKey, 0);
    }

    public String getUserKey() {
        return new String(internalKey, Constants.INTEGER_LENGTH, userKeyLength);
    }

    public String getColumn() {
        return new String(internalKey, Constants.INTEGER_LENGTH + userKeyLength);
    }
    // |userKeyLength|userKey|column
    public int length(){
        return internalKey.length;
    }
}
