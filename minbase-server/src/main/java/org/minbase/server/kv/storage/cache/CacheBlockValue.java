package org.minbase.server.kv.storage.cache;

import org.minbase.server.kv.storage.StoreFile;
import org.minbase.server.kv.storage.block.DataBlock;

import java.util.Objects;

public class CacheBlockValue {
    CacheBlockKey cacheBlockKey;
    DataBlock dataBlock;

    public CacheBlockValue(CacheBlockKey cacheBlockKey, DataBlock dataBlock) {
        this.cacheBlockKey = cacheBlockKey;
        this.dataBlock = dataBlock;
    }

    public CacheBlockKey getCacheBlockKey() {
        return cacheBlockKey;
    }

    public DataBlock getDataBlock() {
        return dataBlock;
    }
}
