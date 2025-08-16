package org.minbase.server.kv.iterator;


import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;

import java.util.LinkedList;
import java.util.Queue;

public abstract class AbstractKeyValueIterator implements KeyValueIterator {
    protected Queue<KeyValue> keyValues = new LinkedList<>();
    protected Key startKey;
    protected Key endKey;

    public AbstractKeyValueIterator(Key startKey, Key endKey) {
        this.startKey = startKey;
        this.endKey = endKey;
        keyValues.add(null);
    }

    @Override
    public KeyValue value() {
        if (keyValues.isEmpty()) {
            return null;
        } else {
            return keyValues.peek();
        }
    }

    /// Get the current key.
    @Override
    public Key key() {
        if (keyValues.isEmpty()) {
            return null;
        } else {
            return keyValues.peek() == null ? null : keyValues.peek().getKey();
        }
    }

    // 锁定到以key开头的[key, ....)的范围
    @Override
    public void seek(Key key) {
        this.startKey = key;
        keyValues.clear();
        keyValues.add(null);
        while (true) {
            KeyValue keyValue = nextInternal();
            if (keyValue == null) {
                if (!hasNextInternal()) {
                    break;
                } else {
                    continue;
                }
            }
            if (inRange(keyValue.getKey())) {
                keyValues.add(keyValue);
                break;
            } else {
                if (endKey != null && (key.compareTo(endKey) >= 0)) {
                    break;
                }
            }
        }
    }


    /// Check if the current iterator is valid.
    @Override
    public boolean hasNext() {
        if (keyValues.size() >= 2) {
            return true;
        } else {
            return hasNextInternal();
        }
    }

    protected abstract boolean hasNextInternal();

    @Override
    public KeyValue next() {
        if (keyValues.size() >= 2) {
            keyValues.poll();
            return value();
        } else {
            boolean hasCurrent = !keyValues.isEmpty();
            KeyValue keyValue = nextInternal();
            if (keyValue != null && inRange(keyValue.getKey())) {
                keyValues.add(keyValue);
            }
            if (hasCurrent) {
                keyValues.poll();
            }
            return value();
        }
    }

    protected abstract KeyValue nextInternal();

    protected boolean inRange(Key key) {
        boolean result = true;
        if (startKey != null) {
            result = result && (key.compareTo(startKey) >= 0);
            if (!result) {
                return result;
            }
        }
        if (endKey != null) {
            result = result && (key.compareTo(endKey) < 0);
        }
        return result;
    }

    public Key getStartKey() {
        return startKey;
    }

    public Key getEndKey() {
        return endKey;
    }
}
