package org.minbase.server.kv.store;


import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.compaction.CompactionScanner;
import org.minbase.server.kv.storage.StoreFile;
import org.minbase.server.kv.storage.StoreFileBuilder;
import org.minbase.server.kv.storage.StoreFileManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedDeque;

public class FlushTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(FlushTask.class);
    ConcurrentLinkedDeque<MemStore> freezedMemStores;
    MemStore lastFreeezedTables;
    StoreFileManager storeFileManager;
    Store store;
    StoreManager storeManager;

    public FlushTask(Store store, StoreManager storeManager) {
        this.storeFileManager = store.getStorageManager();
        this.freezedMemStores = store.getFreezedMemStores();
        this.store = store;
        this.storeManager = storeManager;
    }

    @Override
    public void run() {
        flush();
    }

    public void flush() {
        boolean success = store.flushLock();
        if (!success) {
            return;
        }
        try {
            while (!freezedMemStores.isEmpty()) {
                this.lastFreeezedTables = freezedMemStores.peekLast();
                StoreFileBuilder storeFileBuilder = storeFileManager.newTmpStroeFile();
                MemStoreIterator iterator = lastFreeezedTables.iterator();
                CompactionScanner scanner = new CompactionScanner(iterator, storeManager.getMinReadPoint());

                while (scanner.hasNext()) {
                    KeyValue next = scanner.next();
                    if (next == null) {
                        continue;
                    }
                    storeFileBuilder.add(next);
                }

                StoreFile storeFile = storeFileManager.completeStoreFile(storeFileBuilder);
                freezedMemStores.removeLast();
                long lastSyncSequenceId = lastFreeezedTables.getMaxSecquenceId();
                store.setFlushedSequenceId(lastSyncSequenceId);
                storeManager.requestClearOldLog();
                storeFileManager.cacheDataBlocks(storeFile);
                store.triggerCompaction();
                LOG.debug("Flush freezedMemStore success; memStore:%s, firstKey:%s, lastKey:%s, lastSyncSequenceId:%d",
                        store.getName(), storeFile.getFirstKey(), storeFile.getLastKey(), lastSyncSequenceId);
            }
        } catch (IOException e) {
            LOG.error("Flush freezedMemStore error", e);
            System.exit(-1);
        } finally {
            store.flushUnLock();
        }
    }
}
