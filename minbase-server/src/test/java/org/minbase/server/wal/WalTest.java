package org.minbase.server.wal;


import org.junit.Test;
import org.minbase.server.lsmStorage.LsmStorage;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.op.Value;
import org.minbase.server.utils.ByteUtils;

import java.io.IOException;
import java.util.Scanner;

public class WalTest {
    @Test
    public void test1() throws InterruptedException {
        Wal wal = new Wal(1);
        long i = 0;
        final long startTime = System.currentTimeMillis();
        for (; i < 10 * 100000; i++) {
            Key key = new Key(ByteUtils.toBytes("k" + i), i);
            Value value = Value.Put(ByteUtils.toBytes("v" + i));
            wal.log(new KeyValue(key, value));
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
