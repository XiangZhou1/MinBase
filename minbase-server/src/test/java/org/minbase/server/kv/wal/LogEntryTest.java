package org.minbase.server.kv.wal;

import org.junit.Test;
import org.minbase.server.constant.Constants;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.utils.KeyUtil;
import org.minbase.server.kv.utils.ValueUtil;
import org.minbase.server.kv.wal.LogEntry;
import org.minbase.server.table.TableValue;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.kv.WriteBatch;

import java.nio.charset.StandardCharsets;

public class LogEntryTest {
    private static final String table = "table1";
    private static final String table2 = "table2";

    @Test
    public void test1() {
        KeyValue keyValue = new KeyValue(KeyUtil.latestVersionKey(ByteUtil.toBytes("k1")), ValueUtil.Put(ByteUtil.toBytes("v1")));
        KeyValue keyValue2 = new KeyValue(KeyUtil.latestVersionKey(ByteUtil.toBytes("k2")), ValueUtil.Put(ByteUtil.toBytes("v2")));
        WriteBatch writeBatch = new WriteBatch();
        writeBatch.add(table, keyValue);
        writeBatch.add(table, keyValue2);
        LogEntry logEntry = new LogEntry(writeBatch);
        System.out.println(logEntry);
    }

    @Test
    public void test2() {
        KeyValue keyValue = new KeyValue(KeyUtil.latestVersionKey(ByteUtil.toBytes("k1")), ValueUtil.Put(ByteUtil.toBytes("v1")));
        KeyValue keyValue2 = new KeyValue(KeyUtil.latestVersionKey(ByteUtil.toBytes("k2")), ValueUtil.Put(ByteUtil.toBytes("v2")));
        WriteBatch writeBatch = new WriteBatch();
        writeBatch.add(table, keyValue);
        writeBatch.add(table2, keyValue2);
        LogEntry logEntry = new LogEntry(writeBatch);
        System.out.println(logEntry);

        byte[] encode = logEntry.encode();

        LogEntry logEntry1 = new LogEntry();
        logEntry1.decode(encode);

        assert ByteUtil.ByteEqual(logEntry1.encode(), encode);
    }

}
