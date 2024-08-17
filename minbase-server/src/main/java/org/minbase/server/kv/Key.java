package org.minbase.server.kv;


import org.minbase.common.utils.ByteUtil;
import org.minbase.server.constant.Constants;

import java.io.IOException;
import java.io.OutputStream;

public abstract class Key implements Comparable<Key>  {
    protected long sequenceId;

    public Key() {
    }

    public Key(byte[] key, long sequenceId) {
        setKey(key);
        this.sequenceId = sequenceId;
    }

    public abstract void setKey(byte[] key);

    public abstract byte[] getKey();

    //|key|secquenceId|
    public int length() {
        return getKey().length + Constants.LONG_LENGTH;
    }

    public byte[] encode() {
        byte[] buf = new byte[length()];
        System.arraycopy(getKey(), 0, buf, 0, getKey().length);
        System.arraycopy(ByteUtil.longToByteArray(sequenceId), 0, buf, getKey().length, Constants.LONG_LENGTH);
        return buf;
    }

    public int encodeToFile(OutputStream outputStream) throws IOException {
        byte[] buf = new byte[length()];
        outputStream.write(getKey());
        outputStream.write(ByteUtil.longToByteArray(sequenceId));
        return length();
    }

    public void decode(byte[] buf) {
        byte[] key= new byte[buf.length - Constants.LONG_LENGTH];
        System.arraycopy(buf, 0, key, 0, buf.length - Constants.LONG_LENGTH);
        setKey(key);
        this.sequenceId = ByteUtil.byteArrayToLong(buf, buf.length - Constants.LONG_LENGTH);
    }

    public long getSequenceId() {
        return sequenceId;
    }

    public void setSequenceId(long sequenceId) {
        this.sequenceId = sequenceId;
    }

    @Override
    public int compareTo(Key o2) {
        int result = ByteUtil.BYTE_ORDER_COMPARATOR.compare(this.getKey(), o2.getKey());
        if (result != 0) {
            return result;
        }
        // 同一个userKey, 版本号越大， 则排在前面
        if (this.sequenceId == o2.getSequenceId()) {
            return 0;
        }
        return this.sequenceId > o2.getSequenceId() ? -1 : 1;
    }

    public boolean isLatestVersion() {
        return this.sequenceId == Constants.LATEST_VERSION;
    }

    @Override
    public String toString() {
        return "Key{" +
                "userKey=" + new String(getKey()) +
                ", version=" + sequenceId +
                '}';
    }
}
