package org.minbase.server.lsmStorage;


import org.minbase.server.iterator.MemTableIterator;
import org.minbase.server.mem.MemTable;
import org.minbase.server.storage.sstable.SSTBuilder;
import org.minbase.server.storage.sstable.SSTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedDeque;

public class FlushTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(FlushTask.class);

    ConcurrentLinkedDeque<MemTable> immMemTables;
    MemTable immMemTablesLast;
    StorageManager storageManager;
    LsmStorage lsmStorage;
    public FlushTask(LsmStorage lsmStorage) {
        this.storageManager = lsmStorage.getStorageManager();
        this.immMemTables = lsmStorage.getImmMemTables();
        this.lsmStorage = lsmStorage;
    }

    @Override
    public void run() {
        flush();
    }

    public void flush() {
        synchronized (FlushTask.class) {
            try {
                if (immMemTables.isEmpty()) {
                    return;
                }
                this.immMemTablesLast = immMemTables.peekLast();

                SSTBuilder sstBuilder = new SSTBuilder();
                MemTableIterator iterator = immMemTablesLast.iterator();
                long lastSyncSequenceId = 0;
                while (iterator.isValid()) {
                    lastSyncSequenceId = Math.max(lastSyncSequenceId, iterator.key().getSequenceId()) ;
                    sstBuilder.add(iterator.value());
                    iterator.nextKey();
                }

                SSTable ssTable = sstBuilder.build();
                storageManager.addNewSSTable(ssTable, lastSyncSequenceId);

                immMemTables.removeLast();
                ssTable.cacheDataBlocks();
                lsmStorage.clearOldWal(lastSyncSequenceId);
                lsmStorage.triggerCompaction();
                logger.info("Flush immMemTable success; firstKey =%s, lastKey =%s, lastSyncSequenceId=%d", ssTable.getFirstKey(), ssTable.getLastKey(), lastSyncSequenceId);
            } catch (IOException e) {
                logger.error("Flush immMemTable error", e);
            }
        }
    }
}
