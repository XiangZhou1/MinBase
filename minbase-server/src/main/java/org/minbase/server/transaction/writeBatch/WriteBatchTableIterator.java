package org.minbase.server.transaction.writeBatch;


import org.minbase.server.iterator.KeyIterator;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;

public class WriteBatchTableIterator implements KeyIterator {
    @Override
    public KeyValue value() {
        return null;
    }

    @Override
    public Key key() {
        return null;
    }

    @Override
    public void seek(Key key) {

    }

    @Override
    public boolean isValid() {
        return false;
    }

    @Override
    public void nextKey() {

    }

    @Override
    public void nextUserKey() {

    }
}
