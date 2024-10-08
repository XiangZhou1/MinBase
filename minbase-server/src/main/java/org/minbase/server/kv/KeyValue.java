package org.minbase.server.kv;
import org.minbase.server.constant.Constants;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.factory.KeyFactory;
import org.minbase.server.factory.ValueFactory;

import java.io.IOException;
import java.io.OutputStream;

public class KeyValue {
    private Key key;
    private Value value;

    public KeyValue() {
    }

    public KeyValue(Key key, Value value) {
        this.key = key;
        this.value = value;
    }

    public int length() {
        return key.length() + value.length() + Constants.INTEGER_LENGTH * 2;
    }

    public Key getKey() {
        return key;
    }

    public Value getValue() {
        return value;
    }

    public byte[] encode() {
        byte[] buf = new byte[length()];
        int index = 0;

        System.arraycopy(ByteUtil.intToByteArray(key.length()), 0, buf, index, Constants.INTEGER_LENGTH);
        index += Constants.INTEGER_LENGTH;

        System.arraycopy(key.encode(), 0, buf, index, key.length());
        index += key.length();

        System.arraycopy(ByteUtil.intToByteArray(value.length()), 0, buf, index, Constants.INTEGER_LENGTH);
        index += Constants.INTEGER_LENGTH;

        System.arraycopy(value.encode(), 0, buf, index, value.length());
        return buf;
    }

    public void decode(byte[] bytes, int pos) {
        int keyLen = ByteUtil.byteArrayToInt(bytes, pos);
        pos += Constants.INTEGER_LENGTH;

        byte[] key = new byte[keyLen];
        System.arraycopy(bytes, pos, key, 0, keyLen);
        pos += keyLen;
        this.key = KeyFactory.newKey();
        this.key.decode(key);

        int valueLen = ByteUtil.byteArrayToInt(bytes, pos);
        pos += Constants.INTEGER_LENGTH;

        byte[] value = new byte[valueLen];
        System.arraycopy(bytes, pos, value, 0, valueLen);
        this.value = ValueFactory.newValue();
        this.value.decode(value);
    }

    @Override
    public String toString() {
        return "KeyValue{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }

    public int encodeToFile(OutputStream outputStream) throws IOException {
        int index = 0;
        outputStream.write(ByteUtil.intToByteArray(key.length()));
        index += Constants.INTEGER_LENGTH;

        index += key.encodeToFile(outputStream);

        outputStream.write(ByteUtil.intToByteArray(value.length()));
        index += Constants.INTEGER_LENGTH;

        index += value.encodeToFile(outputStream);
        return index;
    }
}
