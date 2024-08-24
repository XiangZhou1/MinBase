package org.minbase.server.mem;


import org.junit.Before;
import org.junit.Test;
import org.minbase.common.op.Get;
import org.minbase.server.factory.KeyFactory;
import org.minbase.server.iterator.KeyValueIterator;

import org.minbase.server.iterator.MemStoreIterator;
import org.minbase.server.kv.KeyImpl;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.utils.KeyUtils;
import org.minbase.server.utils.ValueUtils;

import java.nio.charset.StandardCharsets;


public class MemStoreTest {
    private static final byte[] column = "cl1".getBytes(StandardCharsets.UTF_8);

    @Before
    public void before() {
        KeyUtils.keyFactory = new KeyFactory(0);
    }

    @Test
    public void test1() {
        MemStore memStore = new MemStore();
        memStore.put(new KeyImpl("k1".getBytes(), 1), ValueUtils.Put("v1".getBytes()));
    }

    @Test
    public void test2() {
        MemStore memStore = new MemStore();
        memStore.put(new KeyImpl("k1".getBytes(), 1), ValueUtils.Put("v1".getBytes()));
        memStore.put(new KeyImpl("k1".getBytes(), 2), ValueUtils.Put("v1_2".getBytes()));

    }

    @Test
    public void test3(){
        MemStore memStore = new MemStore();
        memStore.put(new KeyImpl("k1".getBytes(), 1), ValueUtils.Put("v1".getBytes()));
        memStore.put(new KeyImpl("k1".getBytes(), 2), ValueUtils.Delete());

    }


    @Test
    public void test4() {
        MemStore memStore = new MemStore();
        memStore.put(new KeyImpl("k1".getBytes(), 1), ValueUtils.Put("v1".getBytes()));
        memStore.put(new KeyImpl("k1".getBytes(), 2), ValueUtils.Put("v1_2".getBytes()));

        memStore.put(new KeyImpl("k2".getBytes(), 1), ValueUtils.Put("v2".getBytes()));
        memStore.put(new KeyImpl("k2".getBytes(), 2), ValueUtils.Put("v2_2".getBytes()));

        MemStoreIterator iterator = memStore.iterator();
        while (iterator.isValid()) {
            KeyValue value = iterator.value();
            System.out.println(value);
            iterator.nextInnerKey();
        }
    }


    @Test
    public void test5() {
        MemStore memStore = new MemStore();
        memStore.put(new KeyImpl("k1".getBytes(), 1), ValueUtils.Put("v1".getBytes()));
        memStore.put(new KeyImpl("k1".getBytes(), 2), ValueUtils.Put("v1_2".getBytes()));

        memStore.put(new KeyImpl("k2".getBytes(), 1), ValueUtils.Put("v2".getBytes()));
        memStore.put(new KeyImpl("k2".getBytes(), 2), ValueUtils.Put("v2_2".getBytes()));

        memStore.put(new KeyImpl("k3".getBytes(), 1), ValueUtils.Put("v3".getBytes()));
        memStore.put(new KeyImpl("k3".getBytes(), 2), ValueUtils.Put("v3_2".getBytes()));

        memStore.put(new KeyImpl("k4".getBytes(), 1), ValueUtils.Put("v4".getBytes()));
        memStore.put(new KeyImpl("k4".getBytes(), 2), ValueUtils.Put("v4_2".getBytes()));

        MemStoreIterator iterator = memStore.iterator(KeyUtils.latestKey("k3".getBytes()), null);
        while (iterator.isValid()) {
            KeyValue value = iterator.value();
            System.out.println(value);
            iterator.nextInnerKey();
        }
    }

    @Test
    public void test6() {
        MemStore memStore = new MemStore();
        memStore.put(new KeyImpl("k1".getBytes(), 1), ValueUtils.Put("v1".getBytes()));
        memStore.put(new KeyImpl("k1".getBytes(), 2), ValueUtils.Put("v1_2".getBytes()));

        memStore.put(new KeyImpl("k2".getBytes(), 3), ValueUtils.Put("v2".getBytes()));
        memStore.put(new KeyImpl("k2".getBytes(), 4), ValueUtils.Put("v2_2".getBytes()));

        memStore.put(new KeyImpl("k3".getBytes(), 5), ValueUtils.Put("v3".getBytes()));
        memStore.put(new KeyImpl("k3".getBytes(), 6), ValueUtils.Put("v3_2".getBytes()));

        memStore.put(new KeyImpl("k4".getBytes(), 7), ValueUtils.Put("v4".getBytes()));
        memStore.put(new KeyImpl("k4".getBytes(), 8), ValueUtils.Put("v4_2".getBytes()));

        MemStoreIterator iterator = memStore.iterator(null, null);
        while (iterator.isValid()) {
            System.out.println(iterator.key());
            iterator.nextInnerKey();
        }
        System.out.println("snapshot");
        KeyValueIterator snapshotIterator = memStore.iterator(null, null);
        while (snapshotIterator.isValid()) {
            System.out.println(snapshotIterator.key());
            snapshotIterator.nextInnerKey();
        }
    }
}
