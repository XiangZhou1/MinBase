package org.minbase.server.storage.store;

import org.minbase.common.utils.FileUtil;
import org.minbase.server.iterator.StoreFileIterator;
import org.minbase.server.kv.Key;
import org.minbase.server.storage.block.DataBlock;
import org.minbase.server.storage.block.MetaBlock;
import org.minbase.server.storage.cache.LRUBlockCache;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

public class StoreFileReader {
    private StoreFile storeFile;

    public StoreFileReader(StoreFile storeFile) {
        this.storeFile = storeFile;
    }

    public StoreFile getStoreFile() {
        return storeFile;
    }

    private DataBlock loadBlockFromFile(int index) throws IOException {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(storeFile.getFilePath(), "r")) {
            randomAccessFile.seek(storeFile.getMetaBlock(index).getOffset());

            long blockSize = storeFile.getBlockSize(index);
            byte[] buf = FileUtil.read(randomAccessFile, blockSize);

            DataBlock block = new DataBlock();
            block.setKeyValueNum(storeFile.getMetaBlock(index).getKeyValueNum());
            block.decode(buf);
            return block;
        }
    }

    public StoreFileIterator iterator(Key startKey, Key endKey) {
        return new StoreFileIterator(this, startKey, endKey);
    }

    public StoreFileIterator iterator() {
        return new StoreFileIterator(this, null, null);
    }

    public StoreFileIterator compactionIterator() {
        return new StoreFileIterator(this, null, null, false);
    }


    public DataBlock getBlock(int index, boolean cached) {
        String blockId = storeFile.getBlockId(index);
        DataBlock cache = LRUBlockCache.BlockCache.get(blockId);
        if (cache != null) {
            return cache;
        }

        DataBlock block = null;
        try {
            // 还未加载
            block = loadBlockFromFile(index);
        } catch (Exception e) {
            throw new RuntimeException("read data block fail", e);
        }
        if (cached) {
            block.setBlockId(blockId);
            LRUBlockCache.BlockCache.put(blockId, block);
        }
        return block;
    }


    public ArrayList<MetaBlock> getMetaBlocks() {
        return storeFile.getMetaBlocks();
    }
}
