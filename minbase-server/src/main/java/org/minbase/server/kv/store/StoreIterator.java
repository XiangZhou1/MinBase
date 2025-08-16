package org.minbase.server.kv.store;

import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.iterator.KeyValueIterator;
import org.minbase.server.kv.iterator.MergeIterator;

import java.util.ArrayList;
import java.util.List;

public class StoreIterator implements KeyValueIterator {
    private Store store;
    private MergeIterator mergeIterator;

    public StoreIterator(Store store, Key startKey, Key endKey) {
        this.store = store;
        List<KeyValueIterator> result = new ArrayList<>();
        store.readLock();
        try {
            MemStoreIterator memStoreIterator = store.getMemStore().iterator(startKey, endKey);
            result.add(memStoreIterator);
            for (MemStore freezedMemStore : store.getFreezedMemStores()) {
                MemStoreIterator iterator = freezedMemStore.iterator(startKey, endKey);
                result.add(iterator);
            }
            result.add(store.getStorageManager().newStoreFilesIterator(startKey, endKey, true));
        } finally {
            store.readUnLock();
        }
        this.mergeIterator = new MergeIterator(result);
    }

    @Override
    public KeyValue value() {
        return mergeIterator.value();
    }

    @Override
    public Key key() {
        return mergeIterator.key();
    }

    @Override
    public void seek(Key key) {
        mergeIterator.seek(key);
    }

    @Override
    public boolean hasNext() {
        return mergeIterator.hasNext();
    }

    @Override
    public KeyValue next() {
        return mergeIterator.next();
    }

}
