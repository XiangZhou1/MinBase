package org.minbase.server.op;


import org.minbase.server.constant.Constants;
import org.minbase.common.utils.ByteUtil;

import java.io.IOException;
import java.io.OutputStream;

public class Key implements Comparable<Key> {
    private byte[] userKey;
    private byte[] column;
    private long sequenceId;

    public Key() {
    }

    public Key(byte[] userKey, byte[] column, long sequenceId) {
        this.userKey = userKey;
        this.column = column;
        this.sequenceId = sequenceId;
    }

    public static Key latestKey(byte[] userKey){
        return new Key(userKey, null, Constants.LATEST_VERSION);
    }

    public int length() {
        return userKey.length + Constants.LONG_LENGTH;
    }


    public byte[] encode() {
        byte[] buf = new byte[length()];
        System.arraycopy(userKey, 0, buf, 0, userKey.length);
        System.arraycopy(ByteUtil.longToByteArray(sequenceId), 0, buf, userKey.length, Constants.LONG_LENGTH);
        return buf;
    }

    public int encodeToFile(OutputStream outputStream) throws IOException {
        byte[] buf = new byte[length()];
        outputStream.write(userKey);
        outputStream.write(ByteUtil.longToByteArray(sequenceId));
        return length();
    }

    public void decode(byte[] buf) {
        this.userKey = new byte[buf.length - Constants.LONG_LENGTH];
        System.arraycopy(buf, 0, userKey, 0, buf.length - Constants.LONG_LENGTH);
        this.sequenceId = ByteUtil.byteArrayToLong(buf, buf.length - Constants.LONG_LENGTH);
    }

    public byte[] getUserKey() {
        return userKey;
    }

    public void setUserKey(byte[] userKey) {
        this.userKey = userKey;
    }

    public long getSequenceId() {
        return sequenceId;
    }

    public void setSequenceId(long sequenceId) {
        this.sequenceId = sequenceId;
    }

    @Override
    public int compareTo(Key o2) {
        int result = ByteUtil.BYTE_ORDER_COMPARATOR.compare(this.userKey, o2.userKey);
        if (result != 0) {
            return result;
        }
        // 同一个userKey, 版本号越大， 则排在前面
        if (this.sequenceId == o2.sequenceId) {
            return 0;
        }
        return this.sequenceId > o2.sequenceId ? -1 : 1;
    }

    public boolean isLatestVersion() {
        return this.sequenceId == Constants.LATEST_VERSION;
    }


    public static Key minKey(byte[] userKey) {
        return new Key(userKey, null, Long.MAX_VALUE);
    }

    public static Key maxKey(byte[] userKey) {
        return new Key(userKey, null, Long.MIN_VALUE);
    }

    @Override
    public String toString() {
        return "Key{" +
                "userKey=" + new String(userKey) +
                ", version=" + sequenceId +
                '}';
    }
}
