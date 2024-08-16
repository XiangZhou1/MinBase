package org.minbase.server.compaction;

import org.minbase.server.storage.storemanager.StoreManager;

public interface Compaction {
    void compact(StoreManager storeManager) throws Exception;

    boolean needCompact(StoreManager storeManager);
}
