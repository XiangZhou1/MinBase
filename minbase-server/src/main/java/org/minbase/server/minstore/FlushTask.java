package org.minbase.server.minstore;


import org.minbase.server.iterator.MemStoreIterator;
import org.minbase.server.mem.MemStore;
import org.minbase.server.storage.store.StoreFileBuilder;
import org.minbase.server.storage.store.StoreFile;
import org.minbase.server.storage.storemanager.AbstractStoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedDeque;

public class FlushTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(FlushTask.class);

    ConcurrentLinkedDeque<MemStore> immMemStores;
    MemStore immMemTablesLast;
    AbstractStoreManager storeManager;
    MinStore minStore;

    public FlushTask(MinStore minStore) {
        this.storeManager = minStore.getStorageManager();
        this.immMemStores = minStore.getImmMemTables();
        this.minStore = minStore;
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
                    lastSyncSequenceId = Math.max(lastSyncSequenceId, iterator.key().getSequenceId());
                    storeFileBuilder.add(iterator.value());
                    iterator.next();
                }

                StoreFile storeFile = storeFileBuilder.build();
                storeManager.addStoreFile(storeFile, lastSyncSequenceId);

                immMemStores.removeLast();
                storeFile.cacheDataBlocks();
                //wal.clearOldWal(lastSyncSequenceId);
                minStore.triggerCompaction();
                logger.info("Flush immMemTable success; firstKey =%s, lastKey =%s, lastSyncSequenceId=%d", storeFile.getFirstKey(), storeFile.getLastKey(), lastSyncSequenceId);
            } catch (IOException e) {
                logger.error("Flush immMemTable error", e);
            }
        }
    }
}
