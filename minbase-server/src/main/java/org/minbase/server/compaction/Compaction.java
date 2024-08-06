package org.minbase.server.compaction;

import org.minbase.server.storage.storemanager.AbstractStoreManager;

public interface Compaction {
    void compact(AbstractStoreManager storeManager) throws Exception;

    boolean needCompact(AbstractStoreManager storeManager);
}
