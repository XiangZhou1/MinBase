package org.minbase.server.kv;


import org.minbase.common.utils.ByteUtil;
import org.minbase.server.constant.Constants;

import java.io.IOException;
import java.io.OutputStream;

public class Key implements Comparable<Key> {
    private byte[] internalKey;
    private long version;
    public Key() {
    }

    public Key(byte[] internalKey, long version) {
        this.internalKey = internalKey;
        this.version = version;
    }

    public static Key latestKey(byte[] userKey){
        return new Key(userKey, Constants.LATEST_VERSION);
    }

    public int length() {
        return internalKey.length + Constants.LONG_LENGTH;
    }


    public byte[] encode() {
        byte[] buf = new byte[length()];
        System.arraycopy(internalKey, 0, buf, 0, internalKey.length);
        System.arraycopy(ByteUtil.longToByteArray(version), 0, buf, internalKey.length, Constants.LONG_LENGTH);
        return buf;
    }

    public int encodeToFile(OutputStream outputStream) throws IOException {
        outputStream.write(internalKey);
        outputStream.write(ByteUtil.longToByteArray(version));
        return length();
    }

    public void decode(byte[] buf) {
        this.internalKey = new byte[buf.length - Constants.LONG_LENGTH];
        System.arraycopy(buf, 0, internalKey, 0, buf.length - Constants.LONG_LENGTH);
        this.version = ByteUtil.byteArrayToLong(buf, buf.length - Constants.LONG_LENGTH);
    }

    public byte[] getInternalKey() {
        return internalKey;
    }

    public void setInternalKey(byte[] internalKey) {
        this.internalKey = internalKey;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    @Override
    public int compareTo(Key o2) {
        int result = ByteUtil.BYTE_ORDER_COMPARATOR.compare(this.internalKey, o2.internalKey);
        if (result != 0) {
            return result;
        }
        // 同一个userKey, 版本号越大， 则排在前面
        if (this.version == o2.version) {
            return 0;
        }
        return this.version > o2.version ? -1 : 1;
    }

    public boolean isLatestVersion() {
        return this.version == Constants.LATEST_VERSION;
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
                "userKey=" + new String(internalKey) +
                ", version=" + version +
                '}';
    }
}
