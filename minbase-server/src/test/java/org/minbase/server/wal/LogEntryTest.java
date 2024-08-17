package org.minbase.server.wal;

import org.junit.Test;
import org.minbase.server.constant.Constants;
import org.minbase.server.kv.KeyImpl;
import org.minbase.server.kv.KeyValue;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.transaction.store.WriteBatch;

import java.nio.charset.StandardCharsets;

public class LogEntryTest {
    private static final byte[] column = "cl1".getBytes(StandardCharsets.UTF_8);
    private static final String table = "table1";

    @Test
    public void test1() {
        KeyValue keyValue = new KeyValue(KeyImpl.latestKey(ByteUtil.toBytes("k1")), Value.Put(column, ByteUtil.toBytes("v1")));
        KeyValue keyValue2 = new KeyValue(KeyImpl.latestKey(ByteUtil.toBytes("k2")), Value.Put(column, ByteUtil.toBytes("v2")));
        WriteBatch writeBatch = new WriteBatch();
        writeBatch.add(table, keyValue);
        writeBatch.add(table, keyValue2);
        LogEntry logEntry = new LogEntry(writeBatch);
        System.out.println(logEntry);
    }

    @Test
    public void test2() {
        KeyValue keyValue = new KeyValue(KeyImpl.latestKey(ByteUtil.toBytes("k1")), Value.Put(column, ByteUtil.toBytes("v1")));
        KeyValue keyValue2 = new KeyValue(KeyImpl.latestKey(ByteUtil.toBytes("k2")), Value.Put(column, ByteUtil.toBytes("v2")));
        WriteBatch writeBatch = new WriteBatch();
        writeBatch.add(table, keyValue);
        writeBatch.add(table, keyValue2);
        LogEntry logEntry = new LogEntry(writeBatch);
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
