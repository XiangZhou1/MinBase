package org.minbase.server.wal;


import org.junit.Test;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.op.Value;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.op.WriteBatch;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class WalTest {
    private static final byte[] column = "cl1".getBytes(StandardCharsets.UTF_8);
    private static final String table = "table1";

    @Test
    public void test1() throws InterruptedException {
        Wal wal = new Wal(1);
        long i = 0;
        final long startTime = System.currentTimeMillis();
        for (; i < 10 * 100000; i++) {
            Key key = new Key(ByteUtil.toBytes("k" + i), i);
            Value value = Value.Put(column, ByteUtil.toBytes("v" + i));

            KeyValue keyValue = new KeyValue(Key.latestKey(ByteUtil.toBytes("k1")), Value.Put(column, ByteUtil.toBytes("v1")));
            KeyValue keyValue2 = new KeyValue(Key.latestKey(ByteUtil.toBytes("k2")), Value.Put(column, ByteUtil.toBytes("v2")));
            WriteBatch writeBatch = new WriteBatch();
            writeBatch.add(table, keyValue);
            writeBatch.add(table, keyValue2);
            LogEntry logEntry = new LogEntry(writeBatch);
            writeBatch.setSequenceId(i);
            wal.log(writeBatch);
            if (i % 1000 == 0) {
                System.out.println(i);
            }
        }
        double time = (double)(System.currentTimeMillis() - startTime)/(i+1);
        System.out.println("Per wal cost time:"+time);
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
    }


    @Test
    public void testRecovery() throws IOException {
        Wal wal = new Wal(-1);
        wal.recovery(null);

    }
}
