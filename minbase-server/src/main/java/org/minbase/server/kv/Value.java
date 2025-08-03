package org.minbase.server.kv;

import org.minbase.server.kv.utils.Codec;

import java.io.OutputStream;

public class Value implements Codec {
    private Op op;
    private byte[] value;

    public Value() {
    }

    public Value(Op op) {
        this.op = op;
        this.value = null;
    }

    public int length() {
        return value.length;
    }

    @Override
    public byte[] encode() {
        return new byte[0];
    }

    @Override
    public void decode(byte[] val) {

    }

    public int encodeToFile(OutputStream outputStream) {
        return 0;
    }
}
