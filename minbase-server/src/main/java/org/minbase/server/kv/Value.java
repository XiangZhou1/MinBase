package org.minbase.server.kv;

import java.io.IOException;
import java.io.OutputStream;

public class Value {
    // 0 delete
    // 1 put
    public static byte TYPE_DELETE = -1;
    public static byte TYPE_PUT = 1;
    // put操作还是 delete 操作
    protected byte type;

    private byte[] data = new byte[0];

    public void setData(byte[] data) {
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    public Value() {
    }

    public Value(byte type) {
        this.type = type;
    }

    public Value(byte type, byte[] data) {
        this.type = type;
        setData(data);
    }


    public byte[] encode() {
        byte[] buf = new byte[length()];
        buf[0] = type;
        if (isDelete()) {
            return buf;
        } else if (type == TYPE_PUT) {
            byte[] data = getData();
            System.arraycopy(data, 0, buf, 1, data.length);
        }
        return buf;
    }

    // | type(1)|size(4)|column|column|
    public int encodeToFile(OutputStream outputStream) throws IOException {
        outputStream.write(type);
        if (isDelete()) {
            return 1;
        } else {
            outputStream.write(getData());
        }
        return length();
    }

    public void decode(byte[] buf) {
        type = buf[1];
        if (isDelete()) {
            setData(new byte[0]);
        } else if (type == TYPE_PUT) {
            byte[] data = new byte[buf.length - 1];
            System.arraycopy(buf, 1, data, 0, buf.length - 1);
        }
    }

    public byte type() {
        return type;
    }
    public int length() {
        return getData().length + 1;
    }
    public boolean isDelete() {
        return type == TYPE_DELETE;
    }
    public void setType(byte type) {
        this.type = type;
    }
}
