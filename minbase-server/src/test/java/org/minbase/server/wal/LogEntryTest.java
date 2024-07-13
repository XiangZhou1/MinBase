package org.minbase.server.wal;

import org.junit.Test;
import org.minbase.server.constant.Constants;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.op.Value;
import org.minbase.common.utils.ByteUtils;

import java.util.Arrays;

public class LogEntryTest {
    @Test
    public void test1() {
        KeyValue keyValue = new KeyValue(Key.latestKey(ByteUtils.toBytes("k1")), Value.Put(ByteUtils.toBytes("v1")));
        KeyValue keyValue2 = new KeyValue(Key.latestKey(ByteUtils.toBytes("k2")), Value.Put(ByteUtils.toBytes("v2")));

        LogEntry logEntry = new LogEntry(1, Arrays.asList(keyValue, keyValue2));
        System.out.println(logEntry);
    }

    @Test
    public void test2() {
        KeyValue keyValue = new KeyValue(Key.latestKey(ByteUtils.toBytes("k1")), Value.Put(ByteUtils.toBytes("v1")));
        KeyValue keyValue2 = new KeyValue(Key.latestKey(ByteUtils.toBytes("k2")), Value.Put(ByteUtils.toBytes("v2")));

        LogEntry logEntry = new LogEntry(1, Arrays.asList(keyValue, keyValue2));
        byte[] buf = logEntry.encode();
        byte[] buf2 = new byte[buf.length - Constants.INTEGER_LENGTH];
        System.arraycopy(buf, Constants.INTEGER_LENGTH, buf2, 0, buf2.length);

        LogEntry logEntry2 = new LogEntry();
        logEntry2.decode(buf2);
        System.out.println(logEntry);
        System.out.println(logEntry2);
        assert logEntry.toString().equals(logEntry2.toString());
    }

}
