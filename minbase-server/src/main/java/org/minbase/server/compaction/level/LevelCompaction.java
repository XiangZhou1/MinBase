package org.minbase.server.compaction.level;



import org.minbase.server.compaction.Compaction;
import org.minbase.server.conf.Config;
import org.minbase.server.constant.Constants;
import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.iterator.MergeIterator;
import org.minbase.server.iterator.SSTableIterator;
import org.minbase.server.op.Key;
import org.minbase.server.storage.sstable.SSTBuilder;
import org.minbase.server.storage.sstable.SSTable;
import org.minbase.common.utils.Util;
import org.minbase.server.storage.version.FileEdit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class LevelCompaction implements Compaction {
    private static final Logger logger = LoggerFactory.getLogger(LevelCompaction.class);

    private static final long MAX_SSTABLE_SIZE = Util.parseUnit(Config.get(Constants.KEY_MAX_SSTABLE_SIZE));
    private static final int MAX_LEVEL = 4;
    private LevelStorageManager storageManager;
    /**
     * level0 key范围是可能重叠的
     *  其余level key范围是不会重叠的
     */

    public LevelCompaction(LevelStorageManager storageManager) {
        this.storageManager = storageManager;
    }

    @Override
    public synchronized void compact() throws Exception {
        FileEdit fileEdit = storageManager.newFileEdit();

        for (int i = 0; i < MAX_LEVEL - 1; i++) {
            List<SSTable> ssTables = storageManager.getSSTables(i);
            List<SSTable> ssTables2 = storageManager.getSSTables(i + 1);
            if (ssTables.size() > 3 * ssTables2.size()) {
                compactLevel(i, ssTables.get(ssTables.size() - 1), fileEdit);
            }
        }

        storageManager.applyFileEdit(fileEdit);
    }

    private void compactLevel(int level, SSTable ssTable, FileEdit fileEdit) throws Exception {
        logger.info("Compacting sstable files of level " + level);

        List<KeyValueIterator> ssTableIters = new ArrayList<>();
        SSTableIterator iterator0 = ssTable.compactionIterator();
        ssTableIters.add(iterator0);

        Key firstKey = ssTable.getFirstKey();
        Key lastKey = ssTable.getLastKey();
        ArrayList<SSTable> ssTables2 = chooseCompactSSTable(storageManager.getSSTables(level + 1), firstKey.getUserKey(), lastKey.getUserKey());

        if (ssTables2.isEmpty()) {
            fileEdit.addSSTable(level + 1, ssTable);
            fileEdit.removeSSTable(level, ssTable);
        } else {
            for (SSTable ssTableTemp : ssTables2) {
                SSTableIterator iterator = ssTableTemp.compactionIterator();
                ssTableIters.add(iterator);
            }

            MergeIterator mergeIterator = new MergeIterator(ssTableIters);
            SSTBuilder sstBuilder = new SSTBuilder();
            while (mergeIterator.isValid()) {
                sstBuilder.add(mergeIterator.value());
                mergeIterator.next();
                if (sstBuilder.length() > MAX_SSTABLE_SIZE * (level + 2)) {
                    SSTable newSSTable = sstBuilder.build();
                    storageManager.saveSSTableFile(newSSTable);
                    fileEdit.addSSTable(level + 1, newSSTable);
                    sstBuilder = new SSTBuilder();
                }
            }
            if (sstBuilder.length() != 0) {
                SSTable newSSTable = sstBuilder.build();
                storageManager.saveSSTableFile(newSSTable);
                fileEdit.addSSTable(level + 1, newSSTable);
            }

            fileEdit.removeSSTable(level, ssTable);
            for (SSTable removedTable : ssTables2) {
                fileEdit.removeSSTable(level + 1, removedTable);
            }
        }
    }

    private ArrayList<SSTable> chooseCompactSSTable(List<SSTable> ssTables, byte[] firstKey, byte[] lastKey) {
        ArrayList<SSTable> choosed = new ArrayList<>();
        for (SSTable ssTable : ssTables) {
            if (ssTable.inRange(firstKey, lastKey, true)) {
                choosed.add(ssTable);
            }
        }
        return choosed;
    }

    @Override
    public boolean needCompact() {
        for (int i = 0; i < MAX_LEVEL - 1; i++) {
            List<SSTable> ssTables = storageManager.getSSTables(i);
            List<SSTable> ssTablesNextLevel = storageManager.getSSTables(i + 1);
            if (ssTables.size() > 3 * ssTablesNextLevel.size()) {
                return true;
            }
        }
        return false;
    }
}
