package org.minbase.server.compaction.tiered;


import org.minbase.server.compaction.Compaction;
import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.iterator.MergeIterator;
import org.minbase.server.iterator.StoreFileIterator;
import org.minbase.server.storage.store.StoreFileBuilder;
import org.minbase.server.storage.store.StoreFile;
import org.minbase.server.storage.storemanager.StoreManager;
import org.minbase.server.storage.version.FileEdit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

public class TieredCompaction implements Compaction {
    private static final Logger logger = LoggerFactory.getLogger(TieredCompaction.class);
    public static final int TABLE_SIZE_LIMIT = 5;

    @Override
    public synchronized void compact(StoreManager storeManager) throws Exception {
        FileEdit fileEdit = new FileEdit();
        SortedMap<Integer, List<StoreFile>> levelTables = storeManager.getStoreFiles();
        for (Map.Entry<Integer, List<StoreFile>> entry : levelTables.entrySet()) {
            int level = entry.getKey();
            List<StoreFile> tables = entry.getValue();
            if (tables.size() > 5) {
                compactLevel(level, tables, fileEdit, storeManager);
            }
        }
        storeManager.applyFileEdit(fileEdit);
    }

    private void compactLevel(int level, List<StoreFile> tables, FileEdit fileEdit, StoreManager storeManager) throws Exception {
        logger.info("Compacting sstable files of level " + level);
        List<KeyValueIterator> ssTableIters = new ArrayList<>();

        for (StoreFile storeFileTemp : tables) {
            StoreFileIterator iterator = storeFileTemp.getReader().compactionIterator();
            ssTableIters.add(iterator);
        }

        MergeIterator mergeIterator = new MergeIterator(ssTableIters);
        StoreFileBuilder storeFileBuilder = new StoreFileBuilder();
        while (mergeIterator.isValid()) {
            storeFileBuilder.add(mergeIterator.value());
            mergeIterator.next();
        }

        if (storeFileBuilder.length() != 0) {
            StoreFile newStoreFile = storeFileBuilder.build();
            storeManager.saveStoreFile(newStoreFile);
            fileEdit.addSSTable(level + 1, newStoreFile);
        }

        for (StoreFile removedTable : tables) {
            fileEdit.removeSSTable(level, removedTable);
        }

    }

    @Override
    public boolean needCompact(StoreManager storeManager) {
        SortedMap<Integer, List<StoreFile>> levelTables = storeManager.getStoreFiles();
        for (Map.Entry<Integer, List<StoreFile>> entry : levelTables.entrySet()) {
            List<StoreFile> tables = entry.getValue();
            if (tables.size() > TABLE_SIZE_LIMIT) {
                return true;
            }
        }
        return false;
    }
}
