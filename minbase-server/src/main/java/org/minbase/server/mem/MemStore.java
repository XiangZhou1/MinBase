package org.minbase.server.mem;



import org.minbase.server.conf.Config;
import org.minbase.server.constant.Constants;
import org.minbase.server.iterator.MemStoreIterator;
import org.minbase.server.op.Key;
import org.minbase.server.op.Value;
import org.minbase.common.utils.Util;

import java.util.concurrent.ConcurrentSkipListMap;

public class MemStore {

    private ConcurrentSkipListMap<Key, Value> map;
    private long dataLength = 0;

    public MemStore() {
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
        return dataLength >= Config.MEM_STORE_SIZE_LIMIT;
    }
}
