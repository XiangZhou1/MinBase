package org.minbase.server.wal;



import org.minbase.server.constant.Constants;
import org.minbase.server.op.KeyValue;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.op.WriteBatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 可以把多个KeyValue记录成一条日志, 以保证多个操作的原子性
 * |length|KV|KV|...|
 */
public class LogEntry {
    private WriteBatch writeBatch;
    private int length;
    private long sequenceId;


    public LogEntry(WriteBatch writeBatch) {

    }

    public LogEntry() {

    }


    public int length() {
        return length;
    }

    public byte[] encode() {
//        byte[] buf = new byte[length() + Constants.INTEGER_LENGTH];
//        int pos = 0;
//        System.arraycopy(ByteUtil.intToByteArray(length()), 0, buf, pos, Constants.INTEGER_LENGTH);
//        pos += Constants.INTEGER_LENGTH;
//
//        for (KeyValue keyValue : keyValues) {
//            System.arraycopy(keyValue.encode(), 0, buf, pos, keyValue.length());
//            pos += keyValue.length();
//        }
        return null;
    }

    public void decode(byte[] buf) {
//        int pos = 0;
//        KeyValue keyValue;
//        while (pos < buf.length) {
//            keyValue = new KeyValue();
//            keyValue.decode(buf, pos);
//            pos += keyValue.length();
//            keyValues.add(keyValue);
//        }
//        length = buf.length;
//        sequenceId = keyValues.get(0).getKey().getSequenceId();
    }

    public long getSequenceId() {
        return sequenceId;
    }

    public WriteBatch getWriteBatch() {
        return writeBatch;
    }
}
