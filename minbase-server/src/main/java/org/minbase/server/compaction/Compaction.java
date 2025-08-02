package org.minbase.server.compaction;

import org.minbase.server.storage.storefilemanager.AbstractStoreFileManager;

public interface Compaction {
    void compact(AbstractStoreFileManager storeManager) throws Exception;

    boolean needCompact(AbstractStoreFileManager storeManager);
}
