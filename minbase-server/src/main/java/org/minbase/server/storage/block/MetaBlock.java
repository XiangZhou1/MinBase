package org.minbase.server.storage.block;


import org.minbase.server.constant.Constants;
import org.minbase.server.kv.Key;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.utils.KeyUtils;

import java.io.IOException;
import java.io.OutputStream;

/**
 *  * ---------------------------
 *  *                 | firstKeyLen (Integer)
 *  *  MetaBlock2     | firstKey (byte[])
 *  *                 | lastKeyLen (Integer)
 *  *                 | lastKey (byte[])
 *  *                 | offset (Long)
 *  *                 | KeyValue_num (Integer)
 *  * ---------------------------
 */
public class MetaBlock {
    Key firstKey;
    Key lastKey;
    long offset;
    int keyValueNum;

    public MetaBlock() {
    }

    public MetaBlock(int offset, Key firstKey, Key lastKey, int keyValueNum) {
        this.offset = offset;
        this.firstKey = firstKey;
        this.lastKey = lastKey;
        this.keyValueNum = keyValueNum;
    }

    public long getOffset() {
        return offset;
    }

    public Key getFirstKey() {
        return firstKey;
    }

    public Key getLastKey() {
        return lastKey;
    }

    public int getKeyValueNum() {
        return keyValueNum;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public void setKeyValueNum(int keyValueNum) {
        this.keyValueNum = keyValueNum;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public void setFirstKey(Key firstKey) {
        this.firstKey = firstKey;
    }

    public void setLastKey(Key lastKey) {
        this.lastKey = lastKey;
    }

    public int length() {
        return 3 * Constants.INTEGER_LENGTH + Constants.LONG_LENGTH + firstKey.length() + lastKey.length();
    }

    public void decode(byte[] bytes, int startIndex){
        int pos = startIndex;

        int firstkeyLen = ByteUtil.byteArrayToInt(bytes, pos);
        pos += Constants.INTEGER_LENGTH;
        byte[] firstKeyBuf = new byte[firstkeyLen];
        System.arraycopy(bytes, pos, firstKeyBuf, 0, firstkeyLen);
        this.firstKey = KeyUtils.newKey();
        this.firstKey.decode(firstKeyBuf);
        pos += firstkeyLen;

        int lastkeyLen = ByteUtil.byteArrayToInt(bytes, pos);
        pos += Constants.INTEGER_LENGTH;
        byte[] lastKeyBuf = new byte[lastkeyLen];
        System.arraycopy(bytes, pos, lastKeyBuf, 0, lastkeyLen);
        this.lastKey = KeyUtils.newKey();
        this.lastKey.decode(lastKeyBuf);
        pos += lastkeyLen;

        offset = ByteUtil.byteArrayToLong(bytes, pos);
        pos += Constants.LONG_LENGTH;

        keyValueNum = ByteUtil.byteArrayToInt(bytes, pos);
    }

    public byte[] encode() {
        byte[] bytes = new byte[length()];
        int index = 0;

        System.arraycopy(ByteUtil.intToByteArray(firstKey.length()), 0, bytes, index, Constants.INTEGER_LENGTH);
        index += Constants.INTEGER_LENGTH;
        System.arraycopy(firstKey.encode(), 0, bytes, index, firstKey.length());
        index += firstKey.length();

        System.arraycopy(ByteUtil.intToByteArray(lastKey.length()), 0, bytes, index, Constants.INTEGER_LENGTH);
        index += Constants.INTEGER_LENGTH;
        System.arraycopy(lastKey.encode(), 0, bytes, index, lastKey.length());
        index += lastKey.length();

        System.arraycopy(ByteUtil.longToByteArray(offset), 0, bytes, index, Constants.LONG_LENGTH);
        index += Constants.LONG_LENGTH;

        System.arraycopy(ByteUtil.intToByteArray(keyValueNum), 0, bytes, index, Constants.INTEGER_LENGTH);
        return bytes;
    }

    @Override
    public String toString() {
        return "MetaBlock{" +
                "firstKey=" + firstKey +
                ", lastKey=" + lastKey +
                ", offset=" + offset +
                ", keyValueNum=" + keyValueNum +
                '}';
    }

    public int encodeToFile(OutputStream outputStream) throws IOException {
        int index = 0;

        outputStream.write(ByteUtil.intToByteArray(firstKey.length()));
        index += Constants.INTEGER_LENGTH;

        index += firstKey.encodeToFile(outputStream);

        outputStream.write(ByteUtil.intToByteArray(lastKey.length()));
        index += Constants.INTEGER_LENGTH;

        index += lastKey.encodeToFile(outputStream);

        outputStream.write(ByteUtil.longToByteArray(offset));
        index += Constants.LONG_LENGTH;

        outputStream.write(ByteUtil.intToByteArray(keyValueNum));
        index += Constants.INTEGER_LENGTH;

        return index;
    }
}
