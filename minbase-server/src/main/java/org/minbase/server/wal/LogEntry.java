package org.minbase.server.wal;



import org.minbase.server.constant.Constants;
import org.minbase.server.op.KeyValue;
import org.minbase.common.utils.ByteUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 可以把多个KeyValue记录成一条日志, 以保证多个操作的原子性
 * |length|KV|KV|...|
 */
public class LogEntry {
    private String tableName;
    private List<KeyValue> keyValues;
    private int length;
    private long sequenceId;

    public LogEntry() {
        this.keyValues = new ArrayList<>();
    }

    public LogEntry(List<KeyValue> keyValues) {
        this.keyValues = keyValues;
        this.length = 0;
        keyValues.forEach(keyValue -> {
            this.length += keyValue.length();
        });
    }

    public LogEntry(KeyValue keyValue) {
        this.keyValues = Arrays.asList(keyValue);
        this.length = keyValue.length();
    }

    public void setSequenceId(long sequenceId) {
        this.sequenceId = sequenceId;
        keyValues.forEach(keyValue -> {
            this.length += keyValue.length();
            keyValue.getKey().setSequenceId(sequenceId);
        });
    }

    public int length() {
        return length;
    }

    public byte[] encode() {
        byte[] buf = new byte[length() + Constants.INTEGER_LENGTH];
        int pos = 0;
        System.arraycopy(ByteUtil.intToByteArray(length()), 0, buf, pos, Constants.INTEGER_LENGTH);
        pos += Constants.INTEGER_LENGTH;

        for (KeyValue keyValue : keyValues) {
            System.arraycopy(keyValue.encode(), 0, buf, pos, keyValue.length());
            pos += keyValue.length();
        }
        return buf;
    }

    public void decode(byte[] buf) {
        int pos = 0;
        KeyValue keyValue;
        while (pos < buf.length) {
            keyValue = new KeyValue();
            keyValue.decode(buf, pos);
            pos += keyValue.length();
            keyValues.add(keyValue);
        }
        length = buf.length;
        sequenceId = keyValues.get(0).getKey().getSequenceId();
    }

    public long getSequenceId() {
        return sequenceId;
    }

    public List<KeyValue> getKeyValues() {
        return keyValues;
    }

    @Override
    public String toString() {
        return "LogEntry{" +
                "keyValues=" + keyValues +
                ", length=" + length +
                ", sequenceId=" + sequenceId +
                '}';
    }
}
