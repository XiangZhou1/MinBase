package org.minbase.server.kv.store;

import org.junit.Test;
import org.minbase.server.conf.Configuration;
import org.minbase.server.kv.*;

import java.io.File;
import java.nio.charset.StandardCharsets;

public class StoreManagerTest {

    @Test
    public void testCreate() throws Exception {
        StoreManager storeManager = new StoreManager(new File("data"), new Configuration());
        for (int i = 0; i < 100; i++) {
            Store store = storeManager.createStor("store" + i);
            System.out.println(store);
        }
    }

    @Test
    public void testPut() throws Exception {
        StoreManager storeManager = new StoreManager(new File("data"), new Configuration());
        WriteBatch writeBatch = new WriteBatch();
        for (int i = 0; i < 100; i++) {
            writeBatch.add("store" + i, new KeyValue(new Key(("key" + i).getBytes(StandardCharsets.UTF_8), i + 1),
                    new Value(Op.PUT, ("value" + i).getBytes(StandardCharsets.UTF_8))));
        }
        storeManager.put(writeBatch);
        storeManager.foreFlush();
        Thread.sleep(Long.MAX_VALUE);

    }

    @Test
    public void testGet() throws Exception {
        StoreManager storeManager = new StoreManager(new File("data"), new Configuration());
        for (int i = 0; i < 100; i++) {
            KeyValue keyValue = storeManager.get("store" + i, ("key" + i).getBytes(StandardCharsets.UTF_8));
            assert keyValue != null;
            System.out.println(keyValue.getValue());
        }
    }

}
