package org.minbase.server.kv.store;


import org.minbase.common.conf.Configuration;
import org.minbase.server.constant.Constants;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.Value;

import java.util.concurrent.ConcurrentSkipListMap;

public class MemStore {
    private ConcurrentSkipListMap<Key, KeyValue> map;
    private long dataLength = 0;
    private long memStoreLengthLimit;
    private long maxSecquenceId = -1;
    public MemStore(Configuration conf) {
        this.memStoreLengthLimit = conf.getLong(Constants.MEM_STORE_LENGTH_LIMIT_KEY,
                Constants.MEM_STORE_LENGTH_LIMIT_DEFAULT);

        this.map = new ConcurrentSkipListMap<>();
    }

    public void put(Key key, Value value) {
        KeyValue value1 = new KeyValue(key, value);
        map.put(key, value1);
        dataLength += value1.length();
        this.maxSecquenceId = key.getVersion();
    }

    public void put(KeyValue keyValue) {
        map.put(keyValue.getKey(), keyValue);
        dataLength += keyValue.length();
        this.maxSecquenceId = keyValue.getKey().getVersion();
    }

    public ConcurrentSkipListMap<Key, KeyValue> getMap() {
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

    public long getMaxSecquenceId() {
        return maxSecquenceId;
    }
}
