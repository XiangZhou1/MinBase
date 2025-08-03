package org.minbase.server.kv.store;


import org.minbase.server.kv.iterator.MemStoreIterator;
import org.minbase.server.kv.storage.storefile.StoreFileBuilder;
import org.minbase.server.kv.storage.storefile.StoreFile;
import org.minbase.server.kv.storage.storefilemanager.AbstractStoreFileManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedDeque;

public class FlushTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(FlushTask.class);

    ConcurrentLinkedDeque<MemStore> immMemStores;
    MemStore immMemTablesLast;
    AbstractStoreFileManager storeManager;
    Store store;

    public FlushTask(Store store) {
        this.storeManager = store.getStorageManager();
        this.immMemStores = store.getFreezedMemStores();
        this.store = store;
    }

    @Override
    public void run() {
        flush();
    }

    public void flush() {
        synchronized (FlushTask.class) {
            try {
                if (immMemStores.isEmpty()) {
                    return;
                }
                this.immMemTablesLast = immMemStores.peekLast();

                StoreFileBuilder storeFileBuilder = new StoreFileBuilder();
                MemStoreIterator iterator = immMemTablesLast.iterator();
                long lastSyncSequenceId = 0;
                while (iterator.isValid()) {
                    lastSyncSequenceId = Math.max(lastSyncSequenceId, iterator.key().getVersion());
                    storeFileBuilder.add(iterator.value());
                    iterator.next();
                }

                StoreFile storeFile = storeFileBuilder.build();
                storeManager.addStoreFile(storeFile, lastSyncSequenceId);

                immMemStores.removeLast();
                storeFile.cacheDataBlocks();
                //wal.clearOldWal(lastSyncSequenceId);
                store.triggerCompaction();
                logger.info("Flush immMemTable success; firstKey =%s, lastKey =%s, lastSyncSequenceId=%d", storeFile.getFirstKey(), storeFile.getLastKey(), lastSyncSequenceId);
            } catch (IOException e) {
                logger.error("Flush immMemTable error", e);
            }
        }
    }
}
