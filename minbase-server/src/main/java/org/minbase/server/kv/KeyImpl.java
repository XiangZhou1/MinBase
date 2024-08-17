package org.minbase.server.kv;


public class KeyImpl extends Key {
    private byte[] key;

    public KeyImpl() {
    }

    public KeyImpl(byte[] key, long sequenceId) {
        super(key, sequenceId);
    }

    @Override
    public void setKey(byte[] key) {
        this.key = key;
    }

    @Override
    public byte[] getKey() {
        return key;
    }
}
