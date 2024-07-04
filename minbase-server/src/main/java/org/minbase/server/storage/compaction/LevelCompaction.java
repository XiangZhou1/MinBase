package org.minbase.server.storage.compaction;



import org.minbase.server.conf.Config;
import org.minbase.server.constant.Constants;
import org.minbase.server.iterator.KeyIterator;
import org.minbase.server.iterator.MergeIterator;
import org.minbase.server.iterator.SSTableIterator;
import org.minbase.server.lsmStorage.LevelStorageManager;
import org.minbase.server.lsmStorage.LsmStorage;
import org.minbase.server.op.Key;
import org.minbase.server.storage.sstable.SSTBuilder;
import org.minbase.server.storage.sstable.SSTable;
import org.minbase.server.utils.Utils;
import org.minbase.server.version.EditVersion;

import java.util.ArrayList;
import java.util.List;

public class LevelCompaction implements Compaction {
    private static final long MAX_SSTABLE_SIZE = Utils.parseUnit(Config.get(Constants.KEY_MAX_SSTABLE_SIZE));
    private static final int MAX_LEVEL = 4;
    private LevelStorageManager storageManager;
    /**
     * level0 key范围是可能重叠的
     *  其余level key范围是不会重叠的
     */

    private LsmStorage lsmStorage;

    public LevelCompaction(LevelStorageManager storageManager, LsmStorage lsmStorage) {
        this.storageManager = storageManager;
        this.lsmStorage = lsmStorage;
    }

    @Override
    public synchronized void compact() throws Exception {
        EditVersion editVersion = storageManager.newEditVersion();
        if (shouldCompact()) {
            final List<SSTable> ssTablesL0 = storageManager.getSSTables(0);
            compactLevel(0, ssTablesL0.get(ssTablesL0.size()-1), editVersion);
        }

        for (int i = 1; i < MAX_LEVEL - 1; i++) {
            List<SSTable> ssTables = storageManager.getSSTables(i);
            List<SSTable> ssTablesNextLevel = storageManager.getSSTables(i + 1);
            if (ssTables.size() > 5 * ssTablesNextLevel.size()) {
                compactLevel(i, ssTables.get(ssTables.size()-1), editVersion);
            }
        }

        storageManager.applyEditVersion(editVersion);
    }

    private void compactLevel(int level, SSTable ssTable, EditVersion editVersion) throws Exception {
        System.out.println("compactLevel:" + level);

        List<KeyIterator> ssTableIters = new ArrayList<>();
        SSTableIterator iterator0 = ssTable.compactionIterator();
        ssTableIters.add(iterator0);

        Key startKey = ssTable.getFirstKey();
        Key endKey = ssTable.getLastKey();
        ArrayList<SSTable> choosedNextlevelSsTables = chooseCompactedSSTable(storageManager.getSSTables(level + 1), startKey, endKey);
        for (SSTable ssTableTemp : choosedNextlevelSsTables) {
            SSTableIterator iterator = ssTableTemp.compactionIterator();
            ssTableIters.add(iterator);
        }

        MergeIterator mergeIterator = new MergeIterator(ssTableIters);
        SSTBuilder sstBuilder = new SSTBuilder();
        while (mergeIterator.isValid()) {
            sstBuilder.add(mergeIterator.value());
            mergeIterator.nextKey();
            if (sstBuilder.length() > MAX_SSTABLE_SIZE * (level+2)) {
                SSTable newSSTable = sstBuilder.build();
                storageManager.saveSSTableFile(newSSTable);
                editVersion.addSSTable(level + 1, newSSTable);
                sstBuilder = new SSTBuilder();
            }
        }
        if (sstBuilder.length() != 0) {
            SSTable newSSTable = sstBuilder.build();
            storageManager.saveSSTableFile(newSSTable);
            editVersion.addSSTable(level + 1, newSSTable);
        }

        editVersion.removeSSTable(level, ssTable);
        for (SSTable nextlevelSsTable : choosedNextlevelSsTables) {
            editVersion.removeSSTable(level+1, nextlevelSsTable);
        }
    }

    private ArrayList<SSTable> chooseCompactedSSTable(List<SSTable> ssTables, Key startKey, Key endKey) {
        ArrayList<SSTable> choosed = new ArrayList<>();
        for (SSTable ssTable : ssTables) {
            if (ssTable.inRange(startKey, endKey)){
                choosed.add(ssTable);
            }
        }
        return choosed;
    }

    @Override
    public boolean shouldCompact() {
        int sizeLevel0 = storageManager.getSSTables(0).size();
        int sizeLevel1 = storageManager.getSSTables(1).size();
        return sizeLevel0 > 5 * sizeLevel1;
    }
}
