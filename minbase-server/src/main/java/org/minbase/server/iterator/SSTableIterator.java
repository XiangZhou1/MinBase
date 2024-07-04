package org.minbase.server.iterator;



import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.storage.block.DataBlock;
import org.minbase.server.storage.block.MetaBlock;
import org.minbase.server.storage.sstable.SSTable;
import org.minbase.server.utils.ByteUtils;

import java.util.ArrayList;

public class SSTableIterator implements KeyIterator {
    private SSTable ssTable;
    private int blockIndex = -1;
    private BlockIterator blockIterator;
    private Key startKey;
    private Key endKey;
    private boolean cached;

    public SSTableIterator(SSTable ssTable, Key startKey, Key endKey) {
        this.ssTable = ssTable;
        this.startKey = startKey;
        this.endKey = endKey;
        this.blockIndex = 0;
        this.blockIterator = new BlockIterator(ssTable.getBlock(blockIndex, cached));
        this.cached = true;
        if (this.startKey != null) {
            seek(this.startKey);
        }
    }

    public SSTableIterator(SSTable ssTable, Key startKey, Key endKey, boolean cached) {
        this.ssTable = ssTable;
        this.startKey = startKey;
        this.endKey = endKey;
        this.blockIndex = 0;
        this.blockIterator = new BlockIterator(ssTable.getBlock(blockIndex, cached));
        this.cached = cached;
        if (this.startKey != null) {
            seek(this.startKey);
        }
    }

    @Override
    public void seek(Key key) {
        blockIndex = binarySearchBlock(key);
        if (blockIndex != -1 && blockIndex < ssTable.numOfBlocks()) {
            DataBlock block = ssTable.getBlock(blockIndex, cached);
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
    public boolean isValid() {
        return blockIndex != -1 && blockIterator.isValid();
    }

    @Override
    public void nextKey() {
        blockIterator.nextKey();
        if (!blockIterator.isValid()) {
            if (blockIndex >= ssTable.numOfBlocks() - 1) {
                blockIndex = -1;
                blockIterator = null;
            } else {
                blockIndex++;
                DataBlock block = ssTable.getBlock(blockIndex, cached);
                blockIterator = new BlockIterator(block);
            }
        }
        if (isValid()) {
            if (endKey != null && key().compareTo(endKey) >= 0) {
                blockIndex = -1;
            }
        }
    }

    // 跳到下一个userKey
    @Override
    public void nextUserKey() {
        Key key = key();
        blockIterator.nextUserKey();
        while (!blockIterator.isValid()) {
            if (blockIndex >= ssTable.numOfBlocks() - 1) {
                blockIndex = -1;
                blockIterator = null;
            } else {
                blockIndex++;
                DataBlock block = ssTable.getBlock(blockIndex, cached);
                blockIterator = new BlockIterator(block);
                if (blockIterator.isValid()) {
                    if (ByteUtils.byteEqual(key.getUserKey(), blockIterator.key().getUserKey())) {
                        blockIterator.nextUserKey();
                    }
                }
            }
        }

        if (isValid()) {
            if (endKey != null && key().compareTo(endKey) >= 0) {
                blockIndex = -1;
            }
        }
    }


    // 寻找第一个大于等于该Key的对象
    public int binarySearchBlock(Key key) {
        ArrayList<MetaBlock> array = ssTable.getMetaBlocks();
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
