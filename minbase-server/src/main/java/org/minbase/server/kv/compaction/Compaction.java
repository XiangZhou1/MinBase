package org.minbase.server.kv.compaction;

import org.minbase.server.kv.storage.storefilemanager.AbstractStoreFileManager;

public interface Compaction {
    void compact(AbstractStoreFileManager storeManager) throws Exception;

    boolean needCompact(AbstractStoreFileManager storeManager);
}
