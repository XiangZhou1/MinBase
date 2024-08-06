package org.minbase.server.transaction.store;


import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.op.Value;
import org.minbase.common.utils.ByteUtil;

import java.util.Iterator;
import java.util.Map;

public class TransactionStoreIterator implements KeyValueIterator {
    Iterator<Map.Entry<byte[], Value>> innerIter;
    byte[] startKey;
    byte[] endKey;
    Map.Entry<byte[], Value> entry;

    public TransactionStoreIterator(TransactionStore table, byte[] startKey, byte[] endkey) {
        this.startKey = startKey;
        this.endKey = endkey;
        this.innerIter = table.table.entrySet().iterator();
        if (innerIter.hasNext()) {
            entry = innerIter.next();
        }

        seek(Key.latestKey(startKey));
    }

    @Override
    public KeyValue value() {
        return new KeyValue(Key.latestKey(entry.getKey()), entry.getValue());
    }

    @Override
    public Key key() {
        return Key.latestKey(entry.getKey());
    }

    @Override
    public void seek(Key key) {
        while (isValid() && ByteUtil.byteLess(entry.getKey(), key.getUserKey())) {
            nextInnerKey();
        }
    }

    @Override
    public boolean isValid() {
        return entry != null;
    }

    @Override
    public void nextInnerKey() {
        if (innerIter.hasNext()) {
            entry = innerIter.next();
            if (!ByteUtil.byteLess(entry.getKey(), endKey)) {
                entry = null;
            }
        } else {
            entry = null;
        }
    }

    @Override
    public void next() {
        nextInnerKey();
    }
}
