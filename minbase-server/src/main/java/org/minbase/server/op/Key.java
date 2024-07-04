package org.minbase.server.op;


import org.minbase.server.constant.Constants;
import org.minbase.server.utils.ByteUtils;

public class Key implements Comparable<Key>{
    private byte[] userKey;
    private long sequenceId;

    public Key() {
    }

    public Key(byte[] userKey, long version) {
        this.userKey = userKey;
        this.sequenceId = version;
    }

    public static Key latestKey(byte[] userKey){
        return new Key(userKey, Constants.LATEST_VERSION);
    }

    public int length() {
        return userKey.length + Constants.LONG_LENGTH;
    }


    public byte[] encode() {
        byte[] buf = new byte[length()];
        System.arraycopy(userKey, 0, buf, 0, userKey.length);
        System.arraycopy(ByteUtils.longToByteArray(sequenceId), 0, buf, userKey.length, Constants.LONG_LENGTH);
        return buf;
    }

    public void decode(byte[] buf) {
        this.userKey = new byte[buf.length - Constants.LONG_LENGTH];
        System.arraycopy(buf, 0, userKey, 0, buf.length - Constants.LONG_LENGTH);
        this.sequenceId = ByteUtils.byteArrayToLong(buf, buf.length - Constants.LONG_LENGTH);
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
        int result = ByteUtils.BYTE_ORDER_COMPARATOR.compare(this.userKey, o2.userKey);
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
        return new Key(userKey, Long.MAX_VALUE);
    }

    public static Key maxKey(byte[] userKey) {
        return new Key(userKey, Long.MIN_VALUE);
    }

    @Override
    public String toString() {
        return "Key{" +
                "userKey=" + new String(userKey) +
                ", version=" + sequenceId +
                '}';
    }
}
