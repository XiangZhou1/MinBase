package org.minbase.server.kv.iterator;

import org.junit.Test;
import org.minbase.server.conf.Configuration;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.store.MemStore;
import org.minbase.server.kv.store.MemStoreIterator;
import org.minbase.server.kv.utils.KeyUtil;
import org.minbase.server.kv.utils.ValueUtil;

import java.util.ArrayList;
import java.util.List;

public class MergeIteratorTest {

    public static Configuration configuration = new Configuration();


    @Test
    public void testMerge1() {
        MemStore memStore = new MemStore(configuration);
        memStore.put(new Key("k1".getBytes(), 1), ValueUtil.Put("v1".getBytes()));
        memStore.put(new Key("k1".getBytes(), 2), ValueUtil.Put("v1_2".getBytes()));

        memStore.put(new Key("k2".getBytes(), 1), ValueUtil.Put("v2".getBytes()));
        memStore.put(new Key("k2".getBytes(), 2), ValueUtil.Put("v2_2".getBytes()));

        MemStoreIterator iterator = memStore.iterator();

        MemStore memStore2 = new MemStore(configuration);
        memStore2.put(new Key("k3".getBytes(), 1), ValueUtil.Put("v1".getBytes()));
        memStore2.put(new Key("k3".getBytes(), 2), ValueUtil.Put("v1_2".getBytes()));

        memStore2.put(new Key("k4".getBytes(), 1), ValueUtil.Put("v2".getBytes()));
        memStore2.put(new Key("k4".getBytes(), 2), ValueUtil.Put("v2_2".getBytes()));

        MemStoreIterator iterator2 = memStore2.iterator();

        List<KeyValueIterator> keyValueIterators = new ArrayList<>();
        keyValueIterators.add(iterator);
        keyValueIterators.add(iterator2);
        MergeIterator mergeIterator = new MergeIterator(keyValueIterators);
        while (mergeIterator.hasNext()) {
            KeyValue next = mergeIterator.next();
            System.out.println(next);
        }
    }


    @Test
    public void testMerge2() {
        MemStore memStore = new MemStore(configuration);
        memStore.put(new Key("k1".getBytes(), 1), ValueUtil.Put("v1".getBytes()));
        memStore.put(new Key("k1".getBytes(), 3), ValueUtil.Put("v1_2".getBytes()));

        memStore.put(new Key("k2".getBytes(), 1), ValueUtil.Put("v2".getBytes()));
        memStore.put(new Key("k2".getBytes(), 3), ValueUtil.Put("v2_2".getBytes()));

        MemStoreIterator iterator = memStore.iterator();

        MemStore memStore2 = new MemStore(configuration);
        memStore2.put(new Key("k1".getBytes(), 2), ValueUtil.Put("v1".getBytes()));
        memStore2.put(new Key("k1".getBytes(), 4), ValueUtil.Put("v1_2".getBytes()));

        memStore2.put(new Key("k2".getBytes(), 2), ValueUtil.Put("v2".getBytes()));
        memStore2.put(new Key("k2".getBytes(), 4), ValueUtil.Put("v2_2".getBytes()));

        MemStoreIterator iterator2 = memStore2.iterator();

        List<KeyValueIterator> keyValueIterators = new ArrayList<>();
        keyValueIterators.add(iterator);
        keyValueIterators.add(iterator2);
        MergeIterator mergeIterator = new MergeIterator(keyValueIterators);
        while (mergeIterator.hasNext()) {
            KeyValue next = mergeIterator.next();
            System.out.println(next);
        }
    }

    @Test
    public void testMerge3StartKey() {
        MemStore memStore = new MemStore(configuration);
        memStore.put(new Key("k1".getBytes(), 1), ValueUtil.Put("v1".getBytes()));
        memStore.put(new Key("k1".getBytes(), 3), ValueUtil.Put("v1_2".getBytes()));

        memStore.put(new Key("k2".getBytes(), 1), ValueUtil.Put("v2".getBytes()));
        memStore.put(new Key("k2".getBytes(), 3), ValueUtil.Put("v2_2".getBytes()));

        MemStoreIterator iterator = memStore.iterator();

        MemStore memStore2 = new MemStore(configuration);
        memStore2.put(new Key("k1".getBytes(), 2), ValueUtil.Put("v1".getBytes()));
        memStore2.put(new Key("k1".getBytes(), 4), ValueUtil.Put("v1_2".getBytes()));

        memStore2.put(new Key("k2".getBytes(), 2), ValueUtil.Put("v2".getBytes()));
        memStore2.put(new Key("k2".getBytes(), 4), ValueUtil.Put("v2_2".getBytes()));

        MemStoreIterator iterator2 = memStore2.iterator();

        List<KeyValueIterator> keyValueIterators = new ArrayList<>();
        keyValueIterators.add(iterator);
        keyValueIterators.add(iterator2);
        MergeIterator mergeIterator = new MergeIterator(keyValueIterators, new Key("k2".getBytes(), 4), null);
        while (mergeIterator.hasNext()) {
            KeyValue next = mergeIterator.next();
            System.out.println(next);
        }
    }

    @Test
    public void testMerge4StartKeyEndKey() {
        MemStore memStore = new MemStore(configuration);
        memStore.put(new Key("k1".getBytes(), 1), ValueUtil.Put("v1".getBytes()));
        memStore.put(new Key("k1".getBytes(), 3), ValueUtil.Put("v1_2".getBytes()));

        memStore.put(new Key("k2".getBytes(), 1), ValueUtil.Put("v2".getBytes()));
        memStore.put(new Key("k2".getBytes(), 3), ValueUtil.Put("v2_2".getBytes()));

        MemStoreIterator iterator = memStore.iterator();

        MemStore memStore2 = new MemStore(configuration);
        memStore2.put(new Key("k1".getBytes(), 2), ValueUtil.Put("v1".getBytes()));
        memStore2.put(new Key("k1".getBytes(), 4), ValueUtil.Put("v1_2".getBytes()));

        memStore2.put(new Key("k2".getBytes(), 2), ValueUtil.Put("v2".getBytes()));
        memStore2.put(new Key("k2".getBytes(), 4), ValueUtil.Put("v2_2".getBytes()));

        MemStoreIterator iterator2 = memStore2.iterator();

        List<KeyValueIterator> keyValueIterators = new ArrayList<>();
        keyValueIterators.add(iterator);
        keyValueIterators.add(iterator2);
        MergeIterator mergeIterator = new MergeIterator(keyValueIterators, new Key("k1".getBytes(), 2), new Key("k2".getBytes(), 2));
        while (mergeIterator.hasNext()) {
            KeyValue next = mergeIterator.next();
            System.out.println(next);
        }
    }

    @Test
    public void testMerge5StartKeyEndKey() {
        MemStore memStore = new MemStore(configuration);
        memStore.put(new Key("k1".getBytes(), 1), ValueUtil.Put("v1".getBytes()));
        memStore.put(new Key("k1".getBytes(), 3), ValueUtil.Put("v1_2".getBytes()));

        memStore.put(new Key("k2".getBytes(), 1), ValueUtil.Put("v2".getBytes()));
        memStore.put(new Key("k2".getBytes(), 3), ValueUtil.Put("v2_2".getBytes()));

        MemStoreIterator iterator = memStore.iterator(new Key("k1".getBytes(), 2), new Key("k2".getBytes(), 2));

        MemStore memStore2 = new MemStore(configuration);
        memStore2.put(new Key("k1".getBytes(), 2), ValueUtil.Put("v1".getBytes()));
        memStore2.put(new Key("k1".getBytes(), 4), ValueUtil.Put("v1_2".getBytes()));

        memStore2.put(new Key("k2".getBytes(), 2), ValueUtil.Put("v2".getBytes()));
        memStore2.put(new Key("k2".getBytes(), 4), ValueUtil.Put("v2_2".getBytes()));

        MemStoreIterator iterator2 = memStore2.iterator(new Key("k1".getBytes(), 2), new Key("k2".getBytes(), 2));

        List<KeyValueIterator> keyValueIterators = new ArrayList<>();
        keyValueIterators.add(iterator);
        keyValueIterators.add(iterator2);
        MergeIterator mergeIterator = new MergeIterator(keyValueIterators, new Key("k1".getBytes(), 2), new Key("k2".getBytes(), 2));
        while (mergeIterator.hasNext()) {
            KeyValue next = mergeIterator.next();
            System.out.println(next);
        }
    }

    @Test
    public void testMerge6Seek() {
        MemStore memStore = new MemStore(configuration);
        memStore.put(new Key("k1".getBytes(), 1), ValueUtil.Put("v1".getBytes()));
        memStore.put(new Key("k1".getBytes(), 3), ValueUtil.Put("v1_2".getBytes()));

        memStore.put(new Key("k2".getBytes(), 1), ValueUtil.Put("v2".getBytes()));
        memStore.put(new Key("k2".getBytes(), 3), ValueUtil.Put("v2_2".getBytes()));

        MemStoreIterator iterator = memStore.iterator(null, new Key("k2".getBytes(), 2));

        MemStore memStore2 = new MemStore(configuration);
        memStore2.put(new Key("k1".getBytes(), 2), ValueUtil.Put("v1".getBytes()));
        memStore2.put(new Key("k1".getBytes(), 4), ValueUtil.Put("v1_2".getBytes()));

        memStore2.put(new Key("k2".getBytes(), 2), ValueUtil.Put("v2".getBytes()));
        memStore2.put(new Key("k2".getBytes(), 4), ValueUtil.Put("v2_2".getBytes()));

        MemStoreIterator iterator2 = memStore2.iterator(null, new Key("k2".getBytes(), 2));

        List<KeyValueIterator> keyValueIterators = new ArrayList<>();
        keyValueIterators.add(iterator);
        keyValueIterators.add(iterator2);
        MergeIterator mergeIterator = new MergeIterator(keyValueIterators, null, new Key("k2".getBytes(), 2));
        mergeIterator.seek(new Key("k1".getBytes(), 2));
        while (mergeIterator.hasNext()) {
            KeyValue next = mergeIterator.next();
            System.out.println(next);
        }
    }
}
