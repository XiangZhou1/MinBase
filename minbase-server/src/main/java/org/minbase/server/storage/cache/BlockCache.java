package org.minbase.server.storage.cache;


import org.minbase.server.storage.block.DataBlock;

public interface BlockCache {
    DataBlock get(String blockId);
    void put(String blockId, DataBlock block);
    void evict(String blockId);
    void evict();
}
