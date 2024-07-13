package org.minbase.server.wal;



import org.minbase.server.constant.Constants;
import org.minbase.server.op.KeyValue;
import org.minbase.common.utils.ByteUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * |length|KV|KV|...|
 */
public class LogEntry {
    List<KeyValue> keyValues;

    public LogEntry() {
        keyValues = new ArrayList<>();
    }

    public LogEntry(long currSequenceId, List<KeyValue> keyValues) {
        this.keyValues = keyValues;
        keyValues.forEach(keyValue -> keyValue.getKey().setSequenceId(currSequenceId));
    }

    public int length() {
        int len = 0;
        for (KeyValue keyValue : keyValues) {
            len += keyValue.length();
        }
        return len;
    }


    public byte[] encode() {
        byte[] buf = new byte[length() + Constants.INTEGER_LENGTH];
        int pos = 0;
        System.arraycopy(ByteUtils.intToByteArray(length()), 0, buf, pos, Constants.INTEGER_LENGTH);
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
    }

    public long getSequenceId() {
        return keyValues.get(0).getKey().getSequenceId();
    }

    public List<KeyValue> getKeyValues() {
        return keyValues;
    }

    @Override
    public String toString() {
        return "LogEntry{" +
                "keyValues=" + keyValues +
                '}';
    }
}
