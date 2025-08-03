package org.minbase.server.kv.store;


import org.minbase.server.kv.Key;
import org.minbase.server.kv.Value;
import org.minbase.server.kv.iterator.KeyValueIterator;
import org.minbase.server.kv.KeyValue;

import java.util.Iterator;
import java.util.Map;

public class MemStoreIterator implements KeyValueIterator {
    private MemStore memStore;
    private Iterator<Map.Entry<Key, KeyValue>> iterator;
    private Key startKey;
    private Key endKey;
    private KeyValue entry;

    public MemStoreIterator(MemStore memStore) {
        this(memStore, null, null);
    }

    public MemStoreIterator(MemStore memStore, Key startKey, Key endKey) {
        this.memStore = memStore;
        this.startKey = startKey;
        this.endKey = endKey;
        seek(startKey);
    }


    @Override
    public void seek(Key key) {
        if (key != null && endKey != null) {
            this.iterator = memStore.getMap().subMap(key, true, endKey, false).entrySet().iterator();
        } else if (key != null) {
            this.iterator = memStore.getMap().tailMap(key).entrySet().iterator();
        } else {
            this.iterator = memStore.getMap().entrySet().iterator();
        }
        this.entry = null;
    }

    @Override
    public KeyValue value() {
        if (entry == null) {
            return null;
        }
        return entry;
    }

    @Override
    public Key key() {
        if (entry == null) {
            return null;
        }
        return entry.getKey();
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public void nextInnerKey() {
        // todo
//        if (iterator.hasNext()) {
//            entry = iterator.next();
//        } else {
//            entry = null;
//        }
    }

    // 跳到下一个Key
    @Override
    public KeyValue next() {
        entry = iterator.next().getValue();
        if (entry == null) {
            return null;
        } else {
            return new KeyValue(entry.getKey(), entry.getValue());
        }
    }
}
