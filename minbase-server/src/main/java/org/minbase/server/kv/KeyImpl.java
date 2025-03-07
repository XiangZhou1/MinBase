package org.minbase.server.kv;


import org.minbase.common.utils.ByteUtil;

public class KeyImpl extends Key {
    private byte[] key;

    public KeyImpl() {
    }

    public KeyImpl(byte[] key, long sequenceId) {
        super(key, sequenceId);
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public void setKey(byte[] key) {
        this.key = key;
    }

    @Override
    protected int compareKey(Key key) {
        return ByteUtil.BYTE_ORDER_COMPARATOR.compare(this.key, key.getKey());
    }
}
