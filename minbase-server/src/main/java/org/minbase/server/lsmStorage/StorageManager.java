package org.minbase.server.lsmStorage;

import org.minbase.server.compaction.CompactionStrategy;
import org.minbase.server.compaction.level.LevelStorageManager;
import org.minbase.server.conf.Config;
import org.minbase.server.constant.Constants;
import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.storage.version.ClearOldVersionTask;
import org.minbase.server.storage.version.EditVersion;
import org.minbase.server.storage.version.FileEdit;
import org.minbase.server.storage.sstable.SSTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

public abstract class StorageManager extends ManiFest {
    private static final Logger logger = LoggerFactory.getLogger(LevelStorageManager.class);
    public static final String Data_Dir = Config.get(Constants.KEY_DATA_DIR);

    protected long lastSequenceId = 0;

    protected volatile EditVersion editVersion;
    protected Thread clearOldVersionThread;
    protected CompactionStrategy compactionStrategy;


    public StorageManager() {
        editVersion = new EditVersion();
        clearOldVersionThread = new Thread(new ClearOldVersionTask(this), "ClearOldVersionThread");
        clearOldVersionThread.start();
    }

    public long getLastSequenceId() {
        return lastSequenceId;
    }

    // SSTable 操作
    public abstract void loadSSTables() throws IOException;

    public abstract void addSSTable(SSTable ssTable, long lastSyncSequenceId) throws IOException;

    // 读操作
    public abstract KeyValue get(Key key);

    public abstract KeyValueIterator iterator(Key startKey, Key endKey);

    public CompactionStrategy getCompactionStrategy() {
        return compactionStrategy;
    }

    //=================================================================================================================
    // 文件版本管理
    public synchronized void applyFileEdit(FileEdit fileEdit) {
        final EditVersion editVersion = this.editVersion.applyFileEdit(fileEdit);
        saveManifest();
        this.editVersion = editVersion;
    }

    public synchronized FileEdit newFileEdit() {
        return new FileEdit();
    }

    public synchronized EditVersion getReadEditVersion() {
        this.editVersion.acquireReadReference();
        return this.editVersion;
    }

    @Override
    public synchronized void saveManifest() {
        try {
            try (FileOutputStream outputStream = new FileOutputStream(getManiFestFile())) {
                outputStream.write(("lastSequenceId:" + lastSequenceId + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
                for (Map.Entry<Integer, List<SSTable>> entry : editVersion.getLevelTables().entrySet()) {
                    int level = entry.getKey();
                    outputStream.write((String.valueOf(level) + ":" + entry.getValue().size() + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
                    for (SSTable ssTable : entry.getValue()) {
                        outputStream.write(ssTable.getSsTableId().getBytes(StandardCharsets.UTF_8));
                        outputStream.write(" ".getBytes(StandardCharsets.UTF_8));
                    }
                    outputStream.write(System.lineSeparator().getBytes(StandardCharsets.UTF_8));
                }
                outputStream.flush();
            }
        } catch (IOException e) {
            logger.error("Save manifest error", e);
            System.exit(-1);
        }
    }

    public synchronized EditVersion getEditVersion() {
        return editVersion;
    }

    public synchronized SortedMap<Integer, List<SSTable>> getLevelTables() {
        return editVersion.getLevelTables();
    }
}
