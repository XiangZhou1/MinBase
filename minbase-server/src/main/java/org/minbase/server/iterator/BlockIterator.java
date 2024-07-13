package org.minbase.server.iterator;


import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.storage.block.DataBlock;
import org.minbase.common.utils.ByteUtil;

import java.util.ArrayList;

public class BlockIterator implements KeyValueIterator {
    DataBlock cachedBlock;
    int iterIndex = -1;
    Key startKey;
    Key endKey;

    public BlockIterator(DataBlock block) {
        this(block, null, null);
    }

    public BlockIterator(DataBlock block, Key startKey, Key endKey) {
        this.cachedBlock = block;
        this.startKey = startKey;
        this.endKey = endKey;
        this.iterIndex = 0;

        if (this.startKey != null) {
            seek(this.startKey);
        }
    }

    @Override
    public void seek(Key key) {
        iterIndex = binarySearchKey(key);
    }

    @Override
    public KeyValue value() {
        return cachedBlock.getData().get(iterIndex);
    }

    @Override
    public Key key() {
        return cachedBlock.getData().get(iterIndex).getKey();
    }

    @Override
    public boolean isValid() {
        return iterIndex != -1;
    }

    @Override
    public void nextInnerKey() {
        if (iterIndex >= cachedBlock.getKeyValueNum() - 1) {
            iterIndex = -1;
        } else {
            iterIndex++;
            if (endKey != null && key().compareTo(endKey) >= 0) {
                iterIndex = -1;
            }
        }
    }

    // 寻找第一个大于等于该Key的对象
    public int binarySearchKey(Key key) {
        ArrayList<KeyValue> array = cachedBlock.getData();
        int left = 0;
        int right = array.size() - 1;

        if (key.compareTo(array.get(right).getKey()) > 0) {
            return -1;
        }

        while (left < right) {
            int mid = left + (right - left) / 2;
            int compare = array.get(mid).getKey().compareTo(key);
            if (compare == 0) {
                return mid; // 目标值在数组中的索引
            } else if (compare < 0) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        return left;
    }

    // 跳到下一个userKey
    @Override
    public void next() {
        Key key = key();
        nextInnerKey();
        while (isValid() && ByteUtil.byteEqual(key.getUserKey(), key().getUserKey())) {
            nextInnerKey();
        }
    }
}

