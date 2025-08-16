package org.minbase.server.kv.storage.cache;

import org.minbase.server.kv.storage.StoreFile;

import java.util.Objects;

public class CacheBlockKey {
    StoreFile storeFile;
    int dataBlockIndex;

    public CacheBlockKey(StoreFile storeFile, int dataBlockIndex) {
        this.storeFile = storeFile;
        this.dataBlockIndex = dataBlockIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        CacheBlockKey that = (CacheBlockKey) o;
        return dataBlockIndex == that.dataBlockIndex && Objects.equals(storeFile, that.storeFile);
    }

    @Override
    public int hashCode() {
        return Objects.hash(storeFile, dataBlockIndex);
    }

    public StoreFile getStoreFile() {
        return storeFile;
    }

    public int getDataBlockIndex() {
        return dataBlockIndex;
    }
}
