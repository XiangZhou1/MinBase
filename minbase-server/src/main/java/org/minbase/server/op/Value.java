package org.minbase.server.op;

import java.io.IOException;
import java.io.OutputStream;

public class Value {
    // 0 delete
    // 1 put
    public static byte OPERATION_DELETE_COLUMN = 0;
    public static byte OPERATION_DELETE_ALL = 1;
    public static byte OPERATION_PUT = 2;

    // put操作还是
    private byte operation;
    private byte[] value;

    private static Value DELETE = new Value(OPERATION_DELETE_ALL, null);
    private static Value DELETE_COLUMN = new Value(OPERATION_DELETE_COLUMN, null);

    public Value() {
    }

    public static Value Put(byte[] value) {
        return new Value(OPERATION_PUT, value);
    }

    public static Value Delete() {
        return DELETE;
    }

    public static Value DeleteColumn() {
        return DELETE_COLUMN;
    }

    private Value(byte operation, byte[] value) {
        this.operation = operation;
        this.value = value;
    }

    public byte[] encode() {
        byte[] buf = new byte[1 + (value != null ? value.length : 0)];
        buf[0] = operation;
        if (value != null && value.length != 0) {
            System.arraycopy(value, 0, buf, 1, value.length);
        }
        return buf;
    }

    public int encodeToFile(OutputStream outputStream) throws IOException {
        outputStream.write(operation);
        if (value != null && value.length != 0) {
            outputStream.write(value);
        }
        return length();
    }

    public void decode(byte[] buf) {
        operation = buf[0];
        value = new byte[buf.length - 1];
        System.arraycopy(buf, 1, value, 0, value.length);
    }

    public byte operation() {
        return operation;
    }

    public byte[] value() {
        return value;
    }

    public int length(){
        return 1 + (value != null ? value.length : 0);
    }

    public boolean isDeleteOP() {
        return operation == OPERATION_DELETE;
    }

    @Override
    public String toString() {
        return "Value{" +
                "operation=" + operation +
                ", value=" + (value != null ? new String(value) : "null") +
                '}';
    }
}
