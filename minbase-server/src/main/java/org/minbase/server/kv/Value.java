package org.minbase.server.kv;

import org.minbase.server.constant.Constants;
import org.minbase.server.kv.utils.Codec;

import java.io.IOException;
import java.io.OutputStream;

public class Value implements Codec, Length {
    private Op op;
    private byte[] value;

    public Value() {
    }

    public Value(Op op) {
        this.op = op;
        this.value = null;
    }

    @Override
    public int length() {
        return value.length + Constants.BYTE_LENGTH;
    }

    /**
     * | op | value |
     *
     * @return 解码之后的字节数组
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[length()];
        bytes[0] = op.getOp();
        System.arraycopy(value, 0, bytes, Constants.BYTE_LENGTH, value.length);
        return bytes;
    }

    @Override
    public void decode(byte[] val) {
        byte opVal = val[0];
        if (opVal == Op.PUT.getOp()) {
            this.op = Op.PUT;
        } else if (opVal == Op.DELETE.getOp()) {
            this.op = Op.DELETE;
        }
        this.value = new byte[val.length - 1];
        System.arraycopy(val, Constants.BYTE_LENGTH, value, 0, value.length);
    }

    public int encodeToFile(OutputStream outputStream) throws IOException {
        outputStream.write(op.getOp());
        outputStream.write(value);
        return length();
    }
}
