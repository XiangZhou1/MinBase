package org.minbase.server.kv.store;


import org.junit.Test;
import org.minbase.common.utils.ByteUtil;
import org.minbase.common.utils.Util;
import org.minbase.server.conf.Configuration;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.Op;
import org.minbase.server.kv.Value;
import org.minbase.server.kv.iterator.KeyValueIterator;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class StoreTest {
    public static StoreManager storeManager;

    static {
        storeManager = Mockito.mock(StoreManager.class);
        PowerMockito.when(storeManager.getMinReadPoint()).thenReturn(Long.MAX_VALUE);
    }

    @Test
    public void testCreate() throws IOException {
        String storeName = "store1";
        File storeDir = new File("data", storeName);
        Store store = new Store("store1", storeDir, new Configuration(), storeManager);
        System.out.println(store);
    }


    @Test
    public void testCreateAndPut() throws IOException {
        String storeName = "store1";
        File storeDir = new File("data", storeName);
        Store store = new Store("store1", storeDir, new Configuration(), storeManager);
        System.out.println(store);
        for (int i = 1; i <= 10000; i++) {
            store.put(new KeyValue(new Key(("key" + Util.fillZero(i)).getBytes(StandardCharsets.UTF_8), i),
                    new Value(Op.PUT, ("value" + i).getBytes(StandardCharsets.UTF_8))));
        }
        store.foreFlush();
    }

    @Test
    public void testCreateAndPutAndScan() throws IOException {
        String storeName = "store1";
        File storeDir = new File("data", storeName);
        Store store = new Store("store1", storeDir, new Configuration(), storeManager);
        System.out.println(store);

        KeyValueIterator iterator = store.iterator();
        int i = 1;
        while (iterator.hasNext()) {
            KeyValue next = iterator.next();
            System.out.println(next);
            assert next != null;
            assert ByteUtil.byteEqual(next.getValue().encode(), new Value(Op.PUT, ("value" + i).getBytes(StandardCharsets.UTF_8)).encode());
            i++;
        }
    }

    @Test
    public void testCreateAndPutAndGet() throws IOException {
        String storeName = "store1";
        File storeDir = new File("data", storeName);
        Store store = new Store("store1", storeDir, new Configuration(), storeManager);
        System.out.println(store);

        for (int i = 1; i <= 10000; i++) {
            KeyValue keyValue = store.get(new Key(("key" + Util.fillZero(i)).getBytes(StandardCharsets.UTF_8), Long.MAX_VALUE));
            System.out.println(keyValue);
            assert keyValue != null;
            assert ByteUtil.byteEqual(keyValue.getValue().encode(), new Value(Op.PUT, ("value" + i).getBytes(StandardCharsets.UTF_8)).encode());
        }
    }

    @Test
    public void testCreateAndPutAndCompact() throws Exception {
        String storeName = "store1";
        File storeDir = new File("data", storeName);
        Store store = new Store("store1", storeDir, new Configuration(), storeManager);
        System.out.println(store);
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int j = 0; j < 100000; j++) {
                    for (int i = 1; i <= 10000; i++) {
                        store.put(new KeyValue(new Key(("key" + Util.fillZero(i)).getBytes(StandardCharsets.UTF_8), i),
                                new Value(Op.PUT, ("value" + i).getBytes(StandardCharsets.UTF_8))));
                    }
                    store.foreFlush();
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                System.out.println("thread2 end");
            }
        });
        thread.start();
        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    for (int i = 1; i <= 10000; i++) {
                        KeyValue keyValue = store.get(new Key(("key" + Util.fillZero(i)).getBytes(StandardCharsets.UTF_8), Long.MAX_VALUE));
                        if (keyValue == null) {
                            System.out.println(i);
                            System.out.println(keyValue);
                            assert false;
                        } else {
                            if (!ByteUtil.byteEqual(keyValue.getValue().encode(), new Value(Op.PUT, ("value" + i).getBytes(StandardCharsets.UTF_8)).encode())) {
                                System.out.println(i);
                                System.out.println(keyValue);
                                assert false;
                            }
                        }
                    }
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
        thread2.start();
        Thread.sleep(Long.MAX_VALUE);
    }


}
