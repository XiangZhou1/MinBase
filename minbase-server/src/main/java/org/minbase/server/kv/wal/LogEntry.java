package org.minbase.server.kv.wal;


import org.minbase.common.utils.ByteUtil;
import org.minbase.server.constant.Constants;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.Length;
import org.minbase.server.kv.WriteBatch;

import java.util.List;

/**
 * 可以把多个store的多个KeyValue记录成一条日志, 以保证多个操作的原子性
 * |length(long)|storeNum(int)|store1Namelength(int)|store1Name| keyvalueNum(int)| keyvalue|
 */
public class LogEntry implements Length {
    private WriteBatch writeBatch;
    private int length;
    private long lastSequenceId = -1;
    private long firstSequenceId = Long.MAX_VALUE;


    public LogEntry(WriteBatch writeBatch) {
        this.writeBatch = writeBatch;
        length = 0;
        List<String> storeNames = writeBatch.getStoreNames();
        length += Constants.INTEGER_LENGTH;
        for (String storeName : storeNames) {
            byte[] storeNameBytes = storeName.getBytes();
            length += Constants.INTEGER_LENGTH;
            length += storeNameBytes.length;
            List<KeyValue> keyValues = writeBatch.getKeyValues(storeName);
            length += Constants.INTEGER_LENGTH;
            for (KeyValue keyValue : keyValues) {
                length += keyValue.length();
                this.lastSequenceId = Math.max(lastSequenceId, keyValue.getKey().getVersion());
                this.firstSequenceId = Math.min(firstSequenceId, keyValue.getKey().getVersion());
            }
        }
    }

    public LogEntry() {

    }

    @Override
    public int length() {
        return length;
    }

    /**
     * |length(long)|storeNum(int)|store1Namelength(int)|store1Name| keyvalueNum(int)| keyvalue|
     *
     * @return
     */
    public byte[] encode() {
        byte[] buf = new byte[length()];
        int pos = 0;
        List<String> storeNames = writeBatch.getStoreNames();
        System.arraycopy(ByteUtil.intToByteArray(storeNames.size()), 0, buf, pos, Constants.INTEGER_LENGTH);
        pos += Constants.INTEGER_LENGTH;
        for (String storeName : storeNames) {
            byte[] storeNameBytes = storeName.getBytes();
            System.arraycopy(ByteUtil.intToByteArray(storeNameBytes.length), 0, buf, pos, Constants.INTEGER_LENGTH);
            pos += Constants.INTEGER_LENGTH;
            System.arraycopy(storeNameBytes, 0, buf, pos, storeNameBytes.length);
            pos += storeNameBytes.length;

            List<KeyValue> keyValues = writeBatch.getKeyValues(storeName);
            System.arraycopy(ByteUtil.intToByteArray(keyValues.size()), 0, buf, pos, Constants.INTEGER_LENGTH);
            pos += Constants.INTEGER_LENGTH;
            for (KeyValue keyValue : keyValues) {
                System.arraycopy(keyValue.encode(), 0, buf, pos, keyValue.length());
                pos += keyValue.length();
            }
        }
        return buf;
    }

    /**
     * |storeNum(int)|store1Namelength(int)|store1Name| keyvalueNum(int)| keyvalue|
     * @return
     */
    public void decode(byte[] buf) {
        this.writeBatch = new WriteBatch();
        int pos = 0;
        int storeNum = ByteUtil.byteArrayToInt(buf, pos);
        pos += Constants.INTEGER_LENGTH;
        for (int i = 0; i < storeNum; i++) {
            int storeNameLength = ByteUtil.byteArrayToInt(buf, pos);
            pos += Constants.INTEGER_LENGTH;
            byte[] storeNameBytes = new byte[storeNameLength];
            System.arraycopy(buf, pos, storeNameBytes, 0, storeNameLength);
            pos += storeNameLength;
            String storeName = new String(storeNameBytes);
            int keyValueNum = ByteUtil.byteArrayToInt(buf, pos);
            pos += Constants.INTEGER_LENGTH;
            for (int j = 0; j < keyValueNum; j++) {
                KeyValue keyValue = new KeyValue();
                keyValue.decode(buf, pos);
                pos += keyValue.length();
                writeBatch.add(storeName, keyValue);
                this.lastSequenceId = Math.max(lastSequenceId, keyValue.getKey().getVersion());
                this.firstSequenceId = Math.min(firstSequenceId, keyValue.getKey().getVersion());
            }
        }
        this.length = buf.length;
    }

    public long getLastSequenceId() {
        return lastSequenceId;
    }

    public long getFirstSequenceId() {
        return firstSequenceId;
    }

    public WriteBatch getWriteBatch() {
        return writeBatch;
    }
}
