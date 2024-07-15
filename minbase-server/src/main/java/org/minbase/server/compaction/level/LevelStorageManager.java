package org.minbase.server.compaction.level;


import org.minbase.server.compaction.CompactionStrategy;
import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.iterator.MergeIterator;
import org.minbase.server.iterator.StorageIterator;
import org.minbase.server.lsmStorage.StorageManager;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.storage.version.EditVersion;
import org.minbase.server.storage.version.FileEdit;
import org.minbase.server.storage.sstable.SSTable;
import org.minbase.common.utils.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class LevelStorageManager extends StorageManager {
    private static final Logger logger = LoggerFactory.getLogger(LevelStorageManager.class);

    public LevelStorageManager() {
        super();
        this.compactionStrategy = CompactionStrategy.LEVEL_COMPACTION;
    }

    /////////////////////////////////////////////////////////////////////////////
    // SSTable操作 load/add

    @Override
    public void loadSSTables() throws IOException {
        File file = getManiFestFile();
        if (!file.exists()) {
            return;
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            lastSequenceId = Long.parseLong(reader.readLine().split(":")[1]);
            String line;
            while ((line=reader.readLine()) != null){
                Integer level = Integer.valueOf(line.split(":")[0]);
                final String[] ssTableIds = reader.readLine().split(" ");
                for (String ssTableId : ssTableIds) {
                    SSTable ssTable = loadSSTable(ssTableId);
                    editVersion.getSSTables(level).add(ssTable);
                }
            }
        }
    }

    private RandomAccessFile getSSTableFile(String ssTableId, String mode) throws FileNotFoundException {
        String filePath = getFilePath(ssTableId);
        return new RandomAccessFile(filePath, mode);
    }

    private String getFilePath(String ssTableId) {
        return Data_Dir + File.separator + getCompactionStrategy().toString() + File.separator + ssTableId;
    }

    public SSTable loadSSTable(String ssTableId) throws IOException {
        SSTable ssTable = new SSTable(ssTableId);
        ssTable.setFilePath(getFilePath(ssTableId));
        try (RandomAccessFile ssTableFile = getSSTableFile(ssTableId, "r")) {
            ssTable.loadFromFile(ssTableFile);
        }
        return ssTable;
    }


    @Override
    public void addSSTable(SSTable ssTable, long lastSyncSequenceId) throws IOException {
        // 存到到文件中
        saveSSTableFile(ssTable);
        final FileEdit fileEdit = newFileEdit();
        fileEdit.addSSTable(0, ssTable);
        applyFileEdit(fileEdit);
        this.lastSequenceId = lastSyncSequenceId;
    }

    public void saveSSTableFile(SSTable ssTable) throws IOException {
        File dir = new File(Data_Dir + File.separator + getCompactionStrategy().toString());
        if (!dir.exists()) {
            dir.mkdirs();
        }
        ssTable.setFilePath(getFilePath(ssTable.getSsTableId()));
        try (RandomAccessFile ssTableFile = getSSTableFile(ssTable.getSsTableId(), "rw")) {
            ssTableFile.write(ssTable.encode());
        }
    }

    public List<SSTable> getSSTables(int i) {
        return this.editVersion.getSSTables(i);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // 读写操作

    @Override
    public KeyValue get(Key key) {
        EditVersion currentVersion = getReadEditVersion();
        try {
            for (List<SSTable> ssTables : currentVersion.getLevelTables().values()) {
                ArrayList<KeyValueIterator> list = new ArrayList<>();
                for (SSTable ssTable : ssTables) {
                    if (ssTable.mightContain(key.getUserKey())) {
                        list.add(ssTable.iterator(key, null));
                    }
                }
                MergeIterator mergeIterator = new MergeIterator(list);
                mergeIterator.seek(key);

                if (mergeIterator.isValid()) {
                    if (ByteUtil.byteEqual(key.getUserKey(), mergeIterator.key().getUserKey())) {
                        return mergeIterator.value();
                    }
                }
            }
            return null;
        } finally {
            currentVersion.releaseReadReference();
        }
    }

    @Override
    public KeyValueIterator iterator(Key startKey, Key endKey) {
        ArrayList<KeyValueIterator> list = new ArrayList<>();
        EditVersion currentVersion = getReadEditVersion();
        for (List<SSTable> ssTables : currentVersion.getLevelTables().values()) {
            for (SSTable ssTable : ssTables) {
                if (ssTable.inRange(startKey == null ? null : startKey.getUserKey(), endKey == null ? null : endKey.getUserKey(), false)) {
                    list.add(ssTable.iterator(startKey, endKey));
                }
            }
        }
        return new StorageIterator(list, currentVersion);
    }

    @Override
    public File getManiFestFile() {
        return new File(Data_Dir + File.separator + getCompactionStrategy().toString() + File.separator + manifestFileName);
    }
}
