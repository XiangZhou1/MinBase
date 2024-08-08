package org.minbase.server.iterator;



import org.minbase.server.mem.MemStore;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.op.Value;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemStoreIterator implements KeyValueIterator {
    private MemStore memStore;
    private Iterator<Map.Entry<Key, Value>> iterator;
    private Key startKey;
    private Key endKey;
    private Map.Entry<Key, Value> entry;

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

        if (this.iterator.hasNext()) {
            entry = iterator.next();
        } else {
            entry = null;
        }
    }

    @Override
    public KeyValue value() {
        return new KeyValue(entry.getKey(), entry.getValue());
    }

    @Override
    public Key key() {
        return entry.getKey();
    }

    @Override
    public boolean isValid() {
        return entry != null;
    }

    @Override
    public void nextInnerKey() {
        if (iterator.hasNext()) {
            entry = iterator.next();
        } else {
            entry = null;
        }
    }


    // 跳到下一个userKey
    @Override
    public void next() {
        byte[] userKey = key().getUserKey();
        while (isValid()) {
            nextInnerKey();
            if (isValid()) {
                if (!ByteUtil.byteEqual(userKey, key().getUserKey())) {
                    break;
                }
            }
        }
    }
}
