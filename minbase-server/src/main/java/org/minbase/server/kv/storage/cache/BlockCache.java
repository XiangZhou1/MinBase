package org.minbase.server.kv.storage.cache;


import org.minbase.server.kv.storage.block.DataBlock;

public interface BlockCache {
    DataBlock get(String blockId);
    void put(String blockId, DataBlock block);
    void evict(String blockId);
    void evict();
}
