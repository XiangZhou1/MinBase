package org.minbase.server.kv.storage.storefilemanager;


import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.storage.BlockIterator;
import org.minbase.server.kv.iterator.KeyValueIterator;
import org.minbase.server.kv.storage.block.DataBlock;
import org.minbase.server.kv.storage.block.MetaBlock;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.kv.storage.storefile.StoreFileReader;

import java.util.ArrayList;

public class StoreFileIterator implements KeyValueIterator {
    private StoreFileReader reader;
    private int blockIndex = -1;
    private BlockIterator blockIterator;
    private Key startKey;
    private Key endKey;
    private boolean cached;

    private int numOfBlocks;

    public StoreFileIterator(StoreFileReader reader, Key startKey, Key endKey) {
        this.reader = reader;
        this.startKey = startKey;
        this.endKey = endKey;
        this.blockIndex = 0;
        this.cached = true;
        this.numOfBlocks = reader.getStoreFile().numOfBlocks();
        this.blockIterator = new BlockIterator(reader.getBlock(blockIndex, cached));
        if (this.startKey != null) {
            seek(this.startKey);
        }
    }

    public StoreFileIterator(StoreFileReader reader, Key startKey, Key endKey, boolean cached) {
        this.reader = reader;
        this.startKey = startKey;
        this.endKey = endKey;
        this.blockIndex = 0;
        this.numOfBlocks = reader.getStoreFile().numOfBlocks();
        this.cached = cached;
        this.blockIterator = new BlockIterator(reader.getBlock(blockIndex, cached));
        if (this.startKey != null) {
            seek(this.startKey);
        }
    }

    @Override
    public void seek(Key key) {
        blockIndex = binarySearchBlock(key);
        if (blockIndex != -1 && blockIndex < numOfBlocks) {
            DataBlock block = reader.getBlock(blockIndex, cached);
            blockIterator = new BlockIterator(block);
            blockIterator.seek(key);
        } else {
            blockIndex = -1;
            this.blockIterator = null;
        }
    }

    @Override
    public KeyValue value() {
        return blockIterator.value();
    }

    @Override
    public Key key() {
        return blockIterator.key();
    }

    @Override
    public boolean hasNext() {
        return blockIndex != -1 && blockIterator.hasNext() && (endKey == null || key().compareTo(endKey) < 0);
    }

    @Override
    public void nextInnerKey() {
//        blockIterator.nextInnerKey();
//        if (!blockIterator.hasNext()) {
//            if (blockIndex >= numOfBlocks - 1) {
//                blockIndex = -1;
//                blockIterator = null;
//            } else {
//                blockIndex++;
//                DataBlock block = reader.getBlock(blockIndex, cached);
//                blockIterator = new BlockIterator(block);
//            }
//        }
//        if (hasNext()) {
//            if (endKey != null && key().compareTo(endKey) >= 0) {
//                blockIndex = -1;
//            }
//        }
    }

    // 跳到下一个userKey
    @Override
    public KeyValue next() {
        Key key = key();
        while (!blockIterator.hasNext()) {
            blockIndex++;
            if (blockIndex >= numOfBlocks) {
                return null;
            }
            DataBlock block = reader.getBlock(blockIndex, cached);
            blockIterator = new BlockIterator(block);
        }
        return blockIterator.next();
    }


    // 寻找第一个大于等于该Key的对象
    public int binarySearchBlock(Key key) {
        ArrayList<MetaBlock> array = reader.getMetaBlocks();
        int left = 0;
        int right = array.size() - 1;

        if (key.compareTo(array.get(right).getLastKey()) > 0) {
            return -1;
        }

        while (left < right) {
            int mid = left + (right - left) / 2;

            if (inRange(array.get(mid).getFirstKey(), array.get(mid).getLastKey(), key)) {
                return mid;
            }

            int compare1 = key.compareTo(array.get(mid).getFirstKey());
            if(compare1 <0){
                right = mid;
            }

            int compare2 = key.compareTo(array.get(mid).getLastKey());
            if(compare2 >0){
                left = mid+1;
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
        return key.compareTo(firstKey)>=0 && key.compareTo(lastKey) <=0;
    }

}
