package org.minbase.server.kv.wal;


import org.junit.Test;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.store.StoreManager;
import org.minbase.server.kv.utils.ValueUtil;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.kv.WriteBatch;
import org.minbase.server.table.TableManager;
import org.minbase.server.table.wal.Wal;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class WalTest {
    private static final String table = "table1";
    private static final String table2 = "table2";

    private static final File walDir = new File("data/wal");
    public static StoreManager storeManager;
    public static TableManager tableManager;

    static {
        storeManager = Mockito.mock(StoreManager.class);
        tableManager = Mockito.mock(TableManager.class);
    }

    @Test
    public void test1() throws Exception {
        Wal wal = new Wal(walDir, tableManager);
        KeyValue keyValue = new KeyValue(new Key("k1".getBytes(StandardCharsets.UTF_8), 1), ValueUtil.Put(ByteUtil.toBytes("v1")));
        KeyValue keyValue2 = new KeyValue(new Key("k2".getBytes(StandardCharsets.UTF_8), 2), ValueUtil.Put(ByteUtil.toBytes("v2")));
        WriteBatch writeBatch = new WriteBatch();
        writeBatch.add(table, keyValue);
        writeBatch.add(table2, keyValue2);
        wal.log(writeBatch);
    }


    @Test
    public void testRecovery() throws IOException {
        Wal wal = new Wal(walDir, tableManager);
        wal.recovery();

    }
}
