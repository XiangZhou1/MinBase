package org.minbase.server.kv.storage;


import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.iterator.AbstractKeyValueIterator;
import org.minbase.server.kv.iterator.KeyValueIterator;
import org.minbase.server.kv.storage.block.DataBlock;
import org.minbase.server.kv.storage.block.MetaBlock;

import java.util.ArrayList;

public class StoreFileIterator extends AbstractKeyValueIterator {
    private StoreFileReader reader;
    private int blockIndex = -1;
    private DataBlockIterator dataBlockIterator;
    private boolean cache;
    private int numOfBlocks;

    public StoreFileIterator(StoreFileReader reader) {
        this(reader, null, null, false);
    }

    public StoreFileIterator(StoreFileReader reader, Key startKey, Key endKey, boolean cache) {
        super(startKey, endKey);
        this.reader = reader;
        this.blockIndex = 0;
        this.numOfBlocks = reader.getStoreFile().numOfBlocks();
        this.cache = cache;
        this.dataBlockIterator = new DataBlockIterator(reader.getBlock(blockIndex, cache), startKey, endKey);
        if (this.startKey != null) {
            seek(this.startKey);
        }
    }

    @Override
    protected boolean hasNextInternal() {
        return dataBlockIterator.hasNext() || (blockIndex != -1 && blockIndex < numOfBlocks);
    }

    @Override
    protected KeyValue nextInternal() {
        while (!dataBlockIterator.hasNext()) {
            blockIndex++;
            if (blockIndex >= numOfBlocks) {
                return null;
            }
            DataBlock block = reader.getBlock(blockIndex, cache);
            dataBlockIterator = new DataBlockIterator(block, startKey, endKey);
        }
        return dataBlockIterator.next();
    }

    @Override
    public void seek(Key key) {
        blockIndex = binarySearchBlock(key);
        if (blockIndex != -1 && blockIndex < numOfBlocks) {
            DataBlock block = reader.getBlock(blockIndex, cache);
            dataBlockIterator = new DataBlockIterator(block, startKey, endKey);
            dataBlockIterator.seek(key);
        } else {
            blockIndex = -1;
            this.dataBlockIterator = null;
        }
        keyValues.clear();
        keyValues.add(null);
    }

    // 寻找第一个大于等于该Key的对象
    public int binarySearchBlock(Key key) {
        ArrayList<MetaBlock> array = reader.getMetaBlocks();
        int left = 0;
        int right = array.size() - 1;

        if (key.compareTo(array.get(right).getLastKey()) > 0) {
            return -1;
        }

        while (left + 1 < right) {
            int mid = (left + right) / 2;

            if (inRange(array.get(mid).getFirstKey(), array.get(mid).getLastKey(), key)) {
                return mid;
            }

            int compare1 = key.compareTo(array.get(mid).getFirstKey());
            if (compare1 < 0) {
                right = mid;
            }

            int compare2 = key.compareTo(array.get(mid).getLastKey());
            if (compare2 > 0) {
                left = mid;
            }
        }

//        if (inRange(array.get(left).getFirstKey(), array.get(left).getLastKey(), key)) {
//            return left;
//        } else {
//            return left + 1;
//        }
        return left;
    }


    public static boolean inRange(Key firstKey, Key lastKey, Key key) {
        return key.compareTo(firstKey) >= 0 && key.compareTo(lastKey) <= 0;
    }

}
