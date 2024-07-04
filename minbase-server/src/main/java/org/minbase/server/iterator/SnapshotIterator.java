package org.minbase.server.iterator;


import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;

public class SnapshotIterator implements KeyIterator {
    private KeyIterator iterator;
    private long snapShot;

    public long getSnapShot() {
        return snapShot;
    }

    public SnapshotIterator(KeyIterator iterator, long snapShot) {
        this.iterator = iterator;
        this.snapShot = snapShot;
    }

    @Override
    public KeyValue value() {
        return iterator.value();
    }

    @Override
    public Key key() {
        return iterator.key();
    }

    @Override
    public void seek(Key key) {
        iterator.seek(key);
        checkSnapshot();
    }

    @Override
    public boolean isValid() {
        return iterator.isValid();
    }

    @Override
    public void nextUserKey() {
        iterator.nextUserKey();
        checkSnapshot();
    }

    @Override
    public void nextKey() {
        iterator.nextKey();
        checkSnapshot();
    }

    private void checkSnapshot() {
        while (isValid()) {
            final long sequenceId = key().getSequenceId();
            if (sequenceId > snapShot) {
                iterator.nextKey();
            } else {
                break;
            }
        }
    }
}
