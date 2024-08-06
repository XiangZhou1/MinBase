package org.minbase.server.iterator;



import org.minbase.server.mem.MemStore;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.common.utils.ByteUtil;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemTableIterator implements KeyValueIterator {
    private Iterator<Map.Entry<byte[], ConcurrentSkipListMap<Key, KeyValue>>> iteratorOuter;
    private Map.Entry<byte[], ConcurrentSkipListMap<Key, KeyValue>> entryOuter;
    private Iterator<Map.Entry<Key, KeyValue>> iteratorInner;
    private Map.Entry<Key, KeyValue> entryInner;
    private Key startKey;
    private Key endKey;

    public MemTableIterator(MemStore memStore) {
        this(memStore, null, null);
    }

    public MemTableIterator(MemStore memStore, Key startKey, Key endKey) {
        iteratorOuter = memStore.getEntrySet().iterator();

        this.startKey = startKey;
        this.endKey = endKey;

        nextIterOuter();

        if (startKey != null) {
            seek(this.startKey);
        }

    }

    private void nextIterOuter() {
        if (this.iteratorOuter.hasNext()) {
            entryOuter = iteratorOuter.next();
            iteratorInner = entryOuter.getValue().entrySet().iterator();
            nextIterInner();
        } else {
            entryOuter = null;
            iteratorOuter = null;
        }
    }

    private void nextIterInner() {
        if (this.iteratorInner.hasNext()) {
            entryInner = iteratorInner.next();
        } else {
            entryInner = null;
            iteratorInner = null;
        }
    }

    @Override
    public void seek(Key key) {
        while (isValid() && ByteUtil.byteLess(entryOuter.getKey(), key.getUserKey())) {
            nextIterOuter();
        }
        while (entryInner.getKey().compareTo(key) < 0) {
            nextIterInner();
        }
    }

    @Override
    public KeyValue value() {
        return entryInner.getValue();
    }

    @Override
    public Key key() {
        return entryInner.getKey();
    }

    @Override
    public boolean isValid() {
        return entryOuter != null ;
    }

    @Override
    public void nextInnerKey() {
        nextIterInner();
        checkEndKey();
        if (entryInner == null && entryOuter != null) {
            nextIterOuter();
            checkEndKey();
        }
    }



    private void checkEndKey() {
        if (this.endKey == null) {
            return;
        }
        if (entryInner.getKey().compareTo(endKey) >= 0) {
            entryInner = null;
            entryOuter = null;
            iteratorInner = null;
            iteratorOuter = null;
        }
    }


    // 跳到下一个userKey
    @Override
    public void next() {
        nextIterOuter();
    }
}
