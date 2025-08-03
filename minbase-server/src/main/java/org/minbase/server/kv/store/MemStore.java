package org.minbase.server.kv.store;


import org.minbase.server.conf.Configuration;
import org.minbase.server.constant.Constants;
import org.minbase.server.kv.iterator.MemStoreIterator;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.Value;

import java.util.concurrent.ConcurrentSkipListMap;

public class MemStore {
    private ConcurrentSkipListMap<Key, Value> map;
    private long dataLength = 0;
    private long memStoreLengthLimit;

    public MemStore(Configuration conf) {
        this.memStoreLengthLimit = conf.getLong(Constants.MEM_STORE_LENGTH_LIMIT_KEY,
                Constants.MEM_STORE_LENGTH_LIMIT_DEFAULT);

        this.map = new ConcurrentSkipListMap<>();
    }

    public void put(Key key, Value value) {
        map.put(key, value);
        dataLength += key.length() + value.length();
    }

    public ConcurrentSkipListMap<Key, Value> getMap() {
        return map;
    }

    public MemStoreIterator iterator(Key startKey, Key endKey) {
        return new MemStoreIterator(this, startKey, endKey);
    }

    public MemStoreIterator iterator() {
        return new MemStoreIterator(this, null, null);
    }

    public long getLength() {
        return dataLength;
    }

    public boolean shouldFreeze() {
        return dataLength >= memStoreLengthLimit;
    }
}
