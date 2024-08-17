package org.minbase.server.mem;



import org.minbase.server.conf.Config;
import org.minbase.server.iterator.MemStoreIterator;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.Value;
import org.minbase.server.utils.KeyUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
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

    public KeyValue get(Key key) {
        ConcurrentNavigableMap<Key, Value> navigableMap = map.subMap(KeyUtils.minKey(key.getKey()), true, key, true);
        Map.Entry<Key, Value> entry = navigableMap.lastEntry();
        if (entry == null) {
            return null;
        } else {
            return new KeyValue(entry.getKey(), entry.getValue());
        }
    }
}
