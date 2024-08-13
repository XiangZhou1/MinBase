package org.minbase.server.mem;


import org.junit.Test;
import org.minbase.common.operation.Get;
import org.minbase.server.iterator.KeyValueIterator;

import org.minbase.server.iterator.MemStoreIterator;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.op.Value;

import java.nio.charset.StandardCharsets;


public class MemStoreTest {
    private static final byte[] column = "cl1".getBytes(StandardCharsets.UTF_8);

    @Test
    public void test1() {
        MemStore memStore = new MemStore();
        memStore.put(new Key("k1".getBytes(), 1), Value.Put(column, "v1".getBytes()));
        Get get = new Get("k1".getBytes());

    }

    @Test
    public void test2() {
        MemStore memStore = new MemStore();
        memStore.put(new Key("k1".getBytes(), 1), Value.Put(column, "v1".getBytes()));
        memStore.put(new Key("k1".getBytes(), 2), Value.Put(column, "v1_2".getBytes()));

    }

    @Test
    public void test3(){
        MemStore memStore = new MemStore();
        memStore.put(new Key("k1".getBytes(), 1), Value.Put(column, "v1".getBytes()));
        memStore.put(new Key("k1".getBytes(), 2), Value.Delete());

    }


    @Test
    public void test4() {
        MemStore memStore = new MemStore();
        memStore.put(new Key("k1".getBytes(), 1), Value.Put(column, "v1".getBytes()));
        memStore.put(new Key("k1".getBytes(), 2), Value.Put(column, "v1_2".getBytes()));

        memStore.put(new Key("k2".getBytes(), 1), Value.Put(column, "v2".getBytes()));
        memStore.put(new Key("k2".getBytes(), 2), Value.Put(column, "v2_2".getBytes()));

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
        memStore.put(new Key("k1".getBytes(), 1), Value.Put(column, "v1".getBytes()));
        memStore.put(new Key("k1".getBytes(), 2), Value.Put(column, "v1_2".getBytes()));

        memStore.put(new Key("k2".getBytes(), 1), Value.Put(column, "v2".getBytes()));
        memStore.put(new Key("k2".getBytes(), 2), Value.Put(column, "v2_2".getBytes()));

        memStore.put(new Key("k3".getBytes(), 1), Value.Put(column, "v3".getBytes()));
        memStore.put(new Key("k3".getBytes(), 2), Value.Put(column, "v3_2".getBytes()));

        memStore.put(new Key("k4".getBytes(), 1), Value.Put(column, "v4".getBytes()));
        memStore.put(new Key("k4".getBytes(), 2), Value.Put(column, "v4_2".getBytes()));

        MemStoreIterator iterator = memStore.iterator(Key.latestKey("k3".getBytes()), null);
        while (iterator.isValid()) {
            KeyValue value = iterator.value();
            System.out.println(value);
            iterator.nextInnerKey();
        }
    }

    @Test
    public void test6() {
        MemStore memStore = new MemStore();
        memStore.put(new Key("k1".getBytes(), 1), Value.Put(column, "v1".getBytes()));
        memStore.put(new Key("k1".getBytes(), 2), Value.Put(column, "v1_2".getBytes()));

        memStore.put(new Key("k2".getBytes(), 3), Value.Put(column, "v2".getBytes()));
        memStore.put(new Key("k2".getBytes(), 4), Value.Put(column, "v2_2".getBytes()));

        memStore.put(new Key("k3".getBytes(), 5), Value.Put(column, "v3".getBytes()));
        memStore.put(new Key("k3".getBytes(), 6), Value.Put(column, "v3_2".getBytes()));

        memStore.put(new Key("k4".getBytes(), 7), Value.Put(column, "v4".getBytes()));
        memStore.put(new Key("k4".getBytes(), 8), Value.Put(column, "v4_2".getBytes()));

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
