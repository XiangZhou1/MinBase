package org.minbase.server.compaction.level;



import org.minbase.server.compaction.Compaction;
import org.minbase.server.conf.Config;
import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.iterator.MergeIterator;
import org.minbase.server.iterator.StoreFileIterator;
import org.minbase.server.kv.KeyImpl;
import org.minbase.server.storage.store.StoreFileBuilder;
import org.minbase.server.storage.store.StoreFile;
import org.minbase.server.storage.storemanager.StoreManager;
import org.minbase.server.storage.version.FileEdit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class LevelCompaction implements Compaction {
    private static final Logger logger = LoggerFactory.getLogger(LevelCompaction.class);

    private static final long MAX_SSTABLE_SIZE = 10000;
    private static final int MAX_LEVEL = (int)Config.LEVEL_LIMIT;


    @Override
    public synchronized void compact(StoreManager storeManager) throws Exception {
        FileEdit fileEdit = new FileEdit();

        for (int i = 0; i < MAX_LEVEL - 1; i++) {
            List<StoreFile> storeFiles = storeManager.getStoreFiles(i);
            List<StoreFile> ssTables2 = storeManager.getStoreFiles(i + 1);
            if (storeFiles.size() > 3 * ssTables2.size()) {
                compactLevel(i, storeFiles.get(storeFiles.size() - 1), fileEdit, storeManager);
            }
        }

        storeManager.applyFileEdit(fileEdit);
    }

    private void compactLevel(int level, StoreFile storeFile, FileEdit fileEdit, StoreManager storeManager) throws Exception {
        logger.info("Compacting sstable files of level " + level);

        List<KeyValueIterator> ssTableIters = new ArrayList<>();
        StoreFileIterator iterator0 = storeFile.getReader().compactionIterator();
        ssTableIters.add(iterator0);

        KeyImpl firstKey = storeFile.getFirstKey();
        KeyImpl lastKey = storeFile.getLastKey();
        ArrayList<StoreFile> ssTables2 = chooseCompactSSTable(storeManager.getStoreFiles(level + 1), firstKey.getKey(), lastKey.getKey());

        if (ssTables2.isEmpty()) {
            fileEdit.addSSTable(level + 1, storeFile);
            fileEdit.removeSSTable(level, storeFile);
        } else {
            for (StoreFile storeFileTemp : ssTables2) {
                StoreFileIterator iterator = storeFileTemp.getReader().compactionIterator();
                ssTableIters.add(iterator);
            }

            MergeIterator mergeIterator = new MergeIterator(ssTableIters);
            StoreFileBuilder storeFileBuilder = new StoreFileBuilder();
            while (mergeIterator.isValid()) {
                storeFileBuilder.add(mergeIterator.value());
                mergeIterator.next();
                if (storeFileBuilder.length() > MAX_SSTABLE_SIZE * (level + 2)) {
                    StoreFile newStoreFile = storeFileBuilder.build();
                    storeManager.saveStoreFile(newStoreFile);
                    fileEdit.addSSTable(level + 1, newStoreFile);
                    storeFileBuilder = new StoreFileBuilder();
                }
            }
            if (storeFileBuilder.length() != 0) {
                StoreFile newStoreFile = storeFileBuilder.build();
                storeManager.saveStoreFile(newStoreFile);
                fileEdit.addSSTable(level + 1, newStoreFile);
            }

            fileEdit.removeSSTable(level, storeFile);
            for (StoreFile removedTable : ssTables2) {
                fileEdit.removeSSTable(level + 1, removedTable);
            }
        }
    }

    private ArrayList<StoreFile> chooseCompactSSTable(List<StoreFile> storeFiles, byte[] firstKey, byte[] lastKey) {
        ArrayList<StoreFile> choosed = new ArrayList<>();
        for (StoreFile storeFile : storeFiles) {
            if (storeFile.inRange(firstKey, lastKey, true)) {
                choosed.add(storeFile);
            }
        }
        return choosed;
    }

    @Override
    public boolean needCompact(StoreManager storeManager) {
        for (int i = 0; i < MAX_LEVEL - 1; i++) {
            List<StoreFile> storeFiles = storeManager.getStoreFiles(i);
            List<StoreFile> ssTablesNextLevel = storeManager.getStoreFiles(i + 1);
            if (storeFiles.size() > 3 * ssTablesNextLevel.size()) {
                return true;
            }
        }
        return false;
    }
}
