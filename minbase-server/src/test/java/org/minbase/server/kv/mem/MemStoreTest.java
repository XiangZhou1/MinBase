package org.minbase.server.kv.mem;


import org.junit.Test;
import org.minbase.common.conf.Configuration;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.iterator.KeyValueIterator;

import org.minbase.server.kv.store.MemStoreIterator;
import org.minbase.server.kv.store.MemStore;
import org.minbase.server.kv.utils.KeyUtil;
import org.minbase.server.kv.utils.ValueUtil;


public class MemStoreTest {
    public static Configuration configuration = new Configuration();

    @Test
    public void test1() {
        MemStore memStore = new MemStore(configuration);
        memStore.put(new Key("k1".getBytes(), 1), ValueUtil.Put("v1".getBytes()));
        assert memStore.getMaxSecquenceId() == 1;
    }

    @Test
    public void test2() {
        MemStore memStore = new MemStore(configuration);
        memStore.put(new Key("k1".getBytes(), 1), ValueUtil.Put("v1".getBytes()));
        memStore.put(new Key("k1".getBytes(), 2), ValueUtil.Put("v1_2".getBytes()));
        assert memStore.getMaxSecquenceId() == 2;
    }

    @Test
    public void test3() {
        MemStore memStore = new MemStore(configuration);
        memStore.put(new Key("k1".getBytes(), 1), ValueUtil.Put("v1".getBytes()));
        memStore.put(new Key("k1".getBytes(), 2), ValueUtil.Delete());
    }


    @Test
    public void test4() {
        MemStore memStore = new MemStore(configuration);
        memStore.put(new Key("k1".getBytes(), 1), ValueUtil.Put("v1".getBytes()));
        memStore.put(new Key("k1".getBytes(), 2), ValueUtil.Put("v1_2".getBytes()));

        memStore.put(new Key("k2".getBytes(), 1), ValueUtil.Put("v2".getBytes()));
        memStore.put(new Key("k2".getBytes(), 2), ValueUtil.Put("v2_2".getBytes()));

        MemStoreIterator iterator = memStore.iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
    }


    @Test
    public void test5() {
        MemStore memStore = new MemStore(configuration);
        memStore.put(new Key("k1".getBytes(), 1), ValueUtil.Put("v1".getBytes()));
        memStore.put(new Key("k1".getBytes(), 2), ValueUtil.Put("v1_2".getBytes()));

        memStore.put(new Key("k2".getBytes(), 1), ValueUtil.Put("v2".getBytes()));
        memStore.put(new Key("k2".getBytes(), 2), ValueUtil.Put("v2_2".getBytes()));

        memStore.put(new Key("k3".getBytes(), 1), ValueUtil.Put("v3".getBytes()));
        memStore.put(new Key("k3".getBytes(), 2), ValueUtil.Put("v3_2".getBytes()));

        memStore.put(new Key("k4".getBytes(), 1), ValueUtil.Put("v4".getBytes()));
        memStore.put(new Key("k4".getBytes(), 2), ValueUtil.Put("v4_2".getBytes()));

        MemStoreIterator iterator = memStore.iterator(KeyUtil.latestVersionKey("k3".getBytes()), null);
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
    }

    @Test
    public void test6() {
        MemStore memStore = new MemStore(configuration);
        memStore.put(new Key("k1".getBytes(), 1), ValueUtil.Put("v1".getBytes()));
        memStore.put(new Key("k1".getBytes(), 2), ValueUtil.Put("v1_2".getBytes()));

        memStore.put(new Key("k2".getBytes(), 3), ValueUtil.Put("v2".getBytes()));
        memStore.put(new Key("k2".getBytes(), 4), ValueUtil.Put("v2_2".getBytes()));

        memStore.put(new Key("k3".getBytes(), 5), ValueUtil.Put("v3".getBytes()));
        memStore.put(new Key("k3".getBytes(), 6), ValueUtil.Put("v3_2".getBytes()));

        memStore.put(new Key("k4".getBytes(), 7), ValueUtil.Put("v4".getBytes()));
        memStore.put(new Key("k4".getBytes(), 8), ValueUtil.Put("v4_2".getBytes()));

        MemStoreIterator iterator = memStore.iterator(null, null);
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
        System.out.println("snapshot");
        KeyValueIterator snapshotIterator = memStore.iterator(null, null);
        while (snapshotIterator.hasNext()) {
            System.out.println(snapshotIterator.next());
        }
    }


    @Test
    public void test7Seek() {
        MemStore memStore = new MemStore(configuration);
        memStore.put(new Key("k1".getBytes(), 1), ValueUtil.Put("v1".getBytes()));
        memStore.put(new Key("k1".getBytes(), 2), ValueUtil.Put("v1_2".getBytes()));

        memStore.put(new Key("k2".getBytes(), 3), ValueUtil.Put("v2".getBytes()));
        memStore.put(new Key("k2".getBytes(), 4), ValueUtil.Put("v2_2".getBytes()));

        memStore.put(new Key("k3".getBytes(), 5), ValueUtil.Put("v3".getBytes()));
        memStore.put(new Key("k3".getBytes(), 6), ValueUtil.Put("v3_2".getBytes()));

        memStore.put(new Key("k4".getBytes(), 7), ValueUtil.Put("v4".getBytes()));
        memStore.put(new Key("k4".getBytes(), 8), ValueUtil.Put("v4_2".getBytes()));

        MemStoreIterator iterator = memStore.iterator(new Key("k3".getBytes(), 5), null);
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
    }

    @Test
    public void test8Seek() {
        MemStore memStore = new MemStore(configuration);
        memStore.put(new Key("k1".getBytes(), 1), ValueUtil.Put("v1".getBytes()));
        memStore.put(new Key("k1".getBytes(), 2), ValueUtil.Delete());

        memStore.put(new Key("k2".getBytes(), 3), ValueUtil.Put("v2".getBytes()));
        memStore.put(new Key("k2".getBytes(), 4), ValueUtil.Put("v2_2".getBytes()));

        memStore.put(new Key("k3".getBytes(), 5), ValueUtil.Put("v3".getBytes()));
        memStore.put(new Key("k3".getBytes(), 6), ValueUtil.Put("v3_2".getBytes()));

        memStore.put(new Key("k4".getBytes(), 7), ValueUtil.Put("v4".getBytes()));
        memStore.put(new Key("k4".getBytes(), 8), ValueUtil.Delete());

        MemStoreIterator iterator = memStore.iterator(new Key("k3".getBytes(), 5), null);
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
    }
}
