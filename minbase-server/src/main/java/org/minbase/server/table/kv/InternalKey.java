package org.minbase.server.table.kv;

import org.minbase.common.utils.ByteUtil;
import org.minbase.server.constant.Constants;
import org.minbase.server.kv.Key;

public class InternalKey extends Key {
    private byte[] internalKey;
    private int userKeyLength;

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

    @Override
    public byte[] getKey() {
        return internalKey;
    }

    // |userKeyLength|userKey|column|
    @Override
    public void setKey(byte[] key) {
        this.internalKey = key;
        this.userKeyLength = ByteUtil.byteArrayToInt(key, 0);
    }

    public String getUserKey() {
        return new String(internalKey, Constants.INTEGER_LENGTH, userKeyLength);
    }

    public String getColumn() {
        return new String(internalKey, Constants.INTEGER_LENGTH + userKeyLength,
                internalKey.length - Constants.INTEGER_LENGTH - userKeyLength);
    }

    @Override
    protected int compareKey(Key key) {
        InternalKey key2 = (InternalKey) key;
        if (this.getUserKey().compareTo(key2.getUserKey()) == 0) {
            return 0;
        }
        return this.getColumn().compareTo(key2.getColumn());
    }
}
