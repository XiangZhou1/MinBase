package org.minbase.server.kv.storage.cache;


import org.minbase.server.kv.storage.StoreFile;
import org.minbase.server.kv.storage.block.DataBlock;

public interface BlockCache {
    DataBlock get(StoreFile storeFile, int dataBlockIndex);

    void put(StoreFile storeFile, int dataBlockIndex, DataBlock block);

    void evict(StoreFile storeFile, int dataBlockIndex);

    void evict();
}
