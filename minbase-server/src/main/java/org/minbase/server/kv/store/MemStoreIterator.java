package org.minbase.server.kv.store;


import org.minbase.server.kv.Key;
import org.minbase.server.kv.iterator.AbstractKeyValueIterator;
import org.minbase.server.kv.KeyValue;

import java.util.Iterator;
import java.util.Map;

public class MemStoreIterator extends AbstractKeyValueIterator {
    private MemStore memStore;
    private Iterator<Map.Entry<Key, KeyValue>> iterator;
    public MemStoreIterator(MemStore memStore) {
        this(memStore, null, null);
    }

    public MemStoreIterator(MemStore memStore, Key startKey, Key endKey) {
        super(startKey, endKey);
        this.memStore = memStore;
        seek(startKey);
    }


    @Override
    public void seek(Key key) {
        if (key != null && endKey != null) {
            this.iterator = memStore.getMap().subMap(key, true, endKey, false).entrySet().iterator();
        } else if (key != null && endKey == null) {
            this.iterator = memStore.getMap().tailMap(key).entrySet().iterator();
        } else if (key == null && endKey != null) {
            this.iterator = memStore.getMap().headMap(endKey, false).entrySet().iterator();
        } else {
            this.iterator = memStore.getMap().entrySet().iterator();
        }
        this.keyValues.clear();
        this.keyValues.add(null);
    }

    @Override
    protected boolean hasNextInternal() {
        return iterator.hasNext();
    }

    @Override
    protected KeyValue nextInternal() {
        return iterator.next().getValue();
    }
}
