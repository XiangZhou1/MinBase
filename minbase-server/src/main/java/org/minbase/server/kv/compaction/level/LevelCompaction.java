package org.minbase.server.kv.compaction.level;


import org.minbase.server.kv.compaction.Compaction;
import org.minbase.server.conf.Config;
import org.minbase.server.constant.Constants;
import org.minbase.server.kv.iterator.KeyValueIterator;
import org.minbase.server.kv.iterator.MergeIterator;
import org.minbase.server.kv.iterator.StoreFileIterator;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.storage.storefile.StoreFileBuilder;
import org.minbase.server.kv.storage.storefile.StoreFile;
import org.minbase.common.utils.Util;
import org.minbase.server.kv.storage.storefilemanager.AbstractStoreFileManager;
import org.minbase.server.kv.storage.version.FileEdit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class LevelCompaction implements Compaction {
    private static final Logger logger = LoggerFactory.getLogger(LevelCompaction.class);

    private static final long MAX_SSTABLE_SIZE = Util.parseUnit(Config.get(Constants.KEY_MAX_SSTABLE_SIZE));
    private static final int MAX_LEVEL = 4;


    @Override
    public synchronized void compact(AbstractStoreFileManager storeManager) throws Exception {
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

    private void compactLevel(int level, StoreFile storeFile, FileEdit fileEdit, AbstractStoreFileManager storeManager) throws Exception {
        logger.info("Compacting sstable files of level " + level);

        List<KeyValueIterator> ssTableIters = new ArrayList<>();
        StoreFileIterator iterator0 = storeFile.getReader().compactionIterator();
        ssTableIters.add(iterator0);

        Key firstKey = storeFile.getFirstKey();
        Key lastKey = storeFile.getLastKey();
        ArrayList<StoreFile> ssTables2 = chooseCompactSSTable(storeManager.getStoreFiles(level + 1), firstKey.getInternalKey(), lastKey.getInternalKey());

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
    public boolean needCompact(AbstractStoreFileManager storeManager) {
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
