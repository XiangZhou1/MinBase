package org.minbase.server.kv.storage;


import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.iterator.AbstractKeyValueIterator;
import org.minbase.server.kv.storage.block.DataBlock;

import java.util.ArrayList;

public class DataBlockIterator extends AbstractKeyValueIterator {
    DataBlock cachedBlock;
    int iterIndex = -1;

    public DataBlockIterator(DataBlock block) {
        this(block, null, null);
    }

    public DataBlockIterator(DataBlock block, Key startKey, Key endKey) {
        super(startKey, endKey);
        this.cachedBlock = block;
        this.iterIndex = -1;

        if (this.startKey != null) {
            seek(this.startKey);
        }
    }

    @Override
    protected boolean hasNextInternal() {
        return iterIndex + 1 < cachedBlock.getKeyValueCount() && iterIndex + 1 >= 0;
    }

    @Override
    protected KeyValue nextInternal() {
        iterIndex++;
        if (iterIndex < 0 || iterIndex >= cachedBlock.getKeyValueCount()) {
            return null;
        } else {
            return cachedBlock.getData().get(iterIndex);
        }
    }

    @Override
    public void seek(Key key) {
        iterIndex = -2;
        int newIterIndex = binarySearchFirstGreatOrEqualKey(key);
        if (newIterIndex < 0 || newIterIndex >= cachedBlock.getKeyValueCount()) {
            return;
        }
        iterIndex = newIterIndex - 1;
    }

    // 寻找第一个大于等于该Key的对象
    public int binarySearchFirstGreatOrEqualKey(Key key) {
        ArrayList<KeyValue> array = cachedBlock.getData();
        int left = 0;
        int right = array.size() - 1;

        if (key.compareTo(array.get(right).getKey()) > 0) {
            return array.size();
        }
        if (key.compareTo(array.get(left).getKey()) <= 0) {
            return 0;
        }

        while (left + 1 < right) {
            int mid = (left + right) / 2;
            int compare = key.compareTo(array.get(mid).getKey());
            if (compare == 0) {
                return mid; // 目标值在数组中的索引
            } else if (compare < 0) {
                right = mid;
            } else {
                left = mid;
            }
        }
        return right;
    }
}

