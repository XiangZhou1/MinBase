package org.minbase.server.kv.compaction;

import org.minbase.common.utils.ByteUtil;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.iterator.AbstractKeyValueIterator;
import org.minbase.server.kv.iterator.KeyValueIterator;
import org.minbase.server.kv.utils.ValueUtil;

public class CompactionScanner extends AbstractKeyValueIterator {
    private long minReadPoint;

    private long readPoint;
    KeyValueIterator iterator;
    private KeyValue deletedKeyValue;

    public CompactionScanner(KeyValueIterator iterator, long minReadPoint, Key startKey, Key endKey) {
        super(startKey, endKey);
        this.iterator = iterator;
        this.readPoint = Long.MAX_VALUE;
        this.minReadPoint = minReadPoint;
    }

    public CompactionScanner(KeyValueIterator iterator, long minReadPoint) {
        this(iterator, minReadPoint, null, null);
    }

    @Override
    protected boolean hasNextInternal() {
        return iterator.hasNext();
    }

    @Override
    protected KeyValue nextInternal() {
        KeyValue currentValue = value();
        while (true) {
            if (iterator.hasNext()) {
                KeyValue next = iterator.next();
                if (next == null) {
                    return null;
                }
                // 当比当前的版本大的时候，直接跳过
                if (next.getVersion() > readPoint) {
                    continue;
                }
                if (next.getVersion() >= minReadPoint) {
                    return next;
                }
                if (deletedKeyValue != null &&
                        ByteUtil.byteEqual(deletedKeyValue.getKey().getInternalKey(),
                                next.getKey().getInternalKey())) {
                    continue;
                }
                if (ValueUtil.isDelete(next.getValue())) {
                    deletedKeyValue = next;
                    continue;
                }
                if (currentValue != null &&
                        ByteUtil.byteEqual(currentValue.getKey().getInternalKey(),
                                next.getKey().getInternalKey())) {
                    continue;
                }
                return next;
            } else {
                return null;
            }
        }
    }

    @Override
    public void seek(Key key) {
        iterator.seek(key);
        super.seek(key);
    }

    @Override
    public void close() {
        iterator.close();
    }

    public long getReadPoint() {
        return readPoint;
    }
}
