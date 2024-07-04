package org.minbase.server.lsmStorage;


import org.minbase.server.iterator.MemTableIterator;
import org.minbase.server.mem.MemTable;
import org.minbase.server.storage.sstable.SSTBuilder;
import org.minbase.server.storage.sstable.SSTable;

import java.util.LinkedList;

public class FlushTask implements Runnable{
    LinkedList<MemTable> immMemTables;
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
        synchronized (FlushTask.class) {
            try {
                this.immMemTablesLast = immMemTables.getLast();

                SSTBuilder sstBuilder = new SSTBuilder();
                MemTableIterator iterator = immMemTablesLast.iterator();
                long lastSyncSequenceId = 0;
                while (iterator.isValid()) {
                    lastSyncSequenceId = Math.max(lastSyncSequenceId, iterator.key().getSequenceId()) ;
                    sstBuilder.add(iterator.value());
                    iterator.nextUserKey();
                }

                SSTable ssTable = sstBuilder.build();
                storageManager.addNewSSTable(ssTable, lastSyncSequenceId);

                immMemTables.removeLast();
                ssTable.cacheDataBlock();
                lsmStorage.clearOldWal(lastSyncSequenceId);
                lsmStorage.triggerCompaction();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }
}
