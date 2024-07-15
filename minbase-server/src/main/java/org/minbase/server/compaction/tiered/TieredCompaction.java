package org.minbase.server.compaction.tiered;


import org.minbase.server.compaction.Compaction;
import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.iterator.MergeIterator;
import org.minbase.server.iterator.SSTableIterator;
import org.minbase.server.storage.sstable.SSTBuilder;
import org.minbase.server.storage.sstable.SSTable;
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

    private TieredStorageManager storageManager;

    public TieredCompaction(TieredStorageManager storageManager) {
        this.storageManager = storageManager;
    }

    @Override
    public synchronized void compact() throws Exception {
        FileEdit fileEdit = storageManager.newFileEdit();
        SortedMap<Integer, List<SSTable>> levelTables = storageManager.getLevelTables();
        for (Map.Entry<Integer, List<SSTable>> entry : levelTables.entrySet()) {
            int level = entry.getKey();
            List<SSTable> tables = entry.getValue();
            if (tables.size() > 5) {
                compactLevel(level, tables, fileEdit);
            }
        }
        storageManager.applyFileEdit(fileEdit);
    }

    private void compactLevel(int level, List<SSTable> tables, FileEdit fileEdit) throws Exception {
        logger.info("Compacting sstable files of level " + level);
        List<KeyValueIterator> ssTableIters = new ArrayList<>();

        for (SSTable ssTableTemp : tables) {
            SSTableIterator iterator = ssTableTemp.compactionIterator();
            ssTableIters.add(iterator);
        }

        MergeIterator mergeIterator = new MergeIterator(ssTableIters);
        SSTBuilder sstBuilder = new SSTBuilder();
        while (mergeIterator.isValid()) {
            sstBuilder.add(mergeIterator.value());
            mergeIterator.next();
        }

        if (sstBuilder.length() != 0) {
            SSTable newSSTable = sstBuilder.build();
            storageManager.saveSSTableFile(newSSTable);
            fileEdit.addSSTable(level + 1, newSSTable);
        }

        for (SSTable removedTable : tables) {
            fileEdit.removeSSTable(level, removedTable);
        }

    }

    @Override
    public boolean needCompact() {
        SortedMap<Integer, List<SSTable>> levelTables = storageManager.getLevelTables();
        for (Map.Entry<Integer, List<SSTable>> entry : levelTables.entrySet()) {
            List<SSTable> tables = entry.getValue();
            if (tables.size() > TABLE_SIZE_LIMIT) {
                return true;
            }
        }
        return false;
    }
}
