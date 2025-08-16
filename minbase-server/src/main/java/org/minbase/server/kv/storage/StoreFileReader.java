package org.minbase.server.kv.storage;

import org.minbase.common.utils.FileUtil;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.storage.block.DataBlock;
import org.minbase.server.kv.storage.block.MetaBlock;
import org.minbase.server.kv.storage.cache.BlockCache;
import org.minbase.server.kv.storage.cache.LRUBlockCache;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

public class StoreFileReader {
    private StoreFile storeFile;
    private BlockCache blockCache;

    public void loadNonDataBlocks() throws IOException {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(storeFile.getRawFile(), "r")) {
            storeFile.decodeFromFile(randomAccessFile);
        }
    }

    public BlockCache getBlockCache() {
        return blockCache;
    }

    public void setBlockCache(BlockCache blockCache) {
        this.blockCache = blockCache;
    }

    public StoreFileReader(StoreFile storeFile) {
        this.storeFile = storeFile;
    }

    public StoreFile getStoreFile() {
        return storeFile;
    }

    private DataBlock loadBlockFromFile(int index) throws IOException {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(storeFile.getRawFile(), "r")) {
            randomAccessFile.seek(storeFile.getMetaBlock(index).getOffset());

            long blockSize = storeFile.getBlockSize(index);
            byte[] buf = FileUtil.read(randomAccessFile, blockSize);

            DataBlock block = new DataBlock();
            block.setKeyValueCount(storeFile.getMetaBlock(index).getKeyValueCount());
            block.decode(buf);
            return block;
        }
    }

    public StoreFileIterator iterator(Key startKey, Key endKey) {
        return new StoreFileIterator(this, startKey, endKey, true);
    }

    public StoreFileIterator iterator() {
        return new StoreFileIterator(this, null, null, true);
    }

    public StoreFileIterator compactionIterator() {
        return new StoreFileIterator(this, null, null, false);
    }


    public DataBlock getBlock(int index, boolean cached) {
        String blockId = storeFile.getBlockId(index);
        if (blockCache != null) {
            DataBlock cache = blockCache.get(storeFile, index);
            if (cache != null) {
                return cache;
            }
        }
        DataBlock block = null;
        try {
            // 还未加载
            block = loadBlockFromFile(index);
        } catch (Exception e) {
            throw new RuntimeException("read data block fail", e);
        }
        if (cached && blockCache != null) {
            blockCache.put(storeFile, index, block);
        }
        return block;
    }


    public ArrayList<MetaBlock> getMetaBlocks() {
        return storeFile.getMetaBlocks();
    }
}
