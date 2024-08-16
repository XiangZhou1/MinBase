package org.minbase.server.storage.storemanager;

import org.minbase.server.compaction.CompactionStrategy;
import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.storage.storemanager.level.LevelStoreManager;
import org.minbase.server.storage.version.ClearOldVersionTask;
import org.minbase.server.storage.version.EditVersion;
import org.minbase.server.storage.version.FileEdit;
import org.minbase.server.storage.store.StoreFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

public abstract class StoreManager implements ManiFest {
    private static final Logger logger = LoggerFactory.getLogger(LevelStoreManager.class);

    protected long lastSequenceId = 0;
    protected volatile EditVersion editVersion;
    protected Thread clearOldVersionThread;
    protected CompactionStrategy compactionStrategy;
    protected File storeDir;

    public StoreManager() {
        editVersion = new EditVersion();
        clearOldVersionThread = new Thread(new ClearOldVersionTask(this), "ClearOldVersionThread");
        clearOldVersionThread.start();
    }

    public File getStoreDir() {
        return storeDir;
    }

    public long getLastSequenceId() {
        return lastSequenceId;
    }

    public CompactionStrategy getCompactionStrategy() {
        return compactionStrategy;
    }

    /////////////////////////////////////////////////////////////////////////////
    // SSTable操作 load/add
    public void loadStoreFiles() throws IOException {
        File file = getManiFestFile();
        if (!file.exists()) {
            return;
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            lastSequenceId = Long.parseLong(reader.readLine().split(":")[1]);
            String line;
            while ((line = reader.readLine()) != null) {
                int level = Integer.parseInt(line.split(":")[0]);
                final String[] storeIds = reader.readLine().split(" ");
                for (String storeId : storeIds) {
                    StoreFile storeFile = loadStoreFile(storeId);
                    editVersion.getStoreFiles(level).add(storeFile);
                }
            }
        }
    }

    protected RandomAccessFile getSSTableFile(String storeId, String mode) throws FileNotFoundException {
        String filePath = getFilePath(storeId);
        return new RandomAccessFile(filePath, mode);
    }

    protected String getFilePath(String storeId) {
        return getStoreDir().getPath() + File.separator + getCompactionStrategy().toString() + File.separator + storeId;
    }

    protected StoreFile loadStoreFile(String storeId) throws IOException {
        StoreFile storeFile = new StoreFile(storeId);
        storeFile.setFilePath(getFilePath(storeId));
        try (RandomAccessFile ssTableFile = getSSTableFile(storeId, "r")) {
            storeFile.decodeFromFile(ssTableFile);
        }
        return storeFile;
    }

    public void addStoreFile(StoreFile storeFile, long lastSyncSequenceId) throws IOException {
        // 存到到文件中
        saveStoreFile(storeFile);
        final FileEdit fileEdit = newFileEdit();
        fileEdit.addSSTable(0, storeFile);
        applyFileEdit(fileEdit);
        this.lastSequenceId = lastSyncSequenceId;
    }

    public void saveStoreFile(StoreFile storeFile) throws IOException {
        File dir = new File(getStoreDir() + File.separator + getCompactionStrategy().toString());
        if (!dir.exists()) {
            dir.mkdirs();
        }

        storeFile.setFilePath(getFilePath(storeFile.getStoreId()));
        try (BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(storeFile.getFilePath()))) {
            storeFile.encodeToFile(outputStream);
            outputStream.flush();
        }
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

    public synchronized void saveManifest() {
        try {
            try (FileOutputStream outputStream = new FileOutputStream(getManiFestFile())) {
                outputStream.write(("lastSequenceId:" + lastSequenceId + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
                for (Map.Entry<Integer, List<StoreFile>> entry : editVersion.getStoreFiles().entrySet()) {
                    int level = entry.getKey();
                    outputStream.write((level + ":" + entry.getValue().size() + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
                    for (StoreFile storeFile : entry.getValue()) {
                        outputStream.write(storeFile.getStoreId().getBytes(StandardCharsets.UTF_8));
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

    public File getManiFestFile() {
        return new File(getStoreDir(), File.separator + getCompactionStrategy().toString() + File.separator + manifestFileName);
    }

    public synchronized EditVersion getEditVersion(boolean read) {
        EditVersion editVersion = this.editVersion;
        if (read) {
            editVersion.acquireReadReference();
        }
        return editVersion;
    }

    public synchronized SortedMap<Integer, List<StoreFile>> getStoreFiles() {
        return editVersion.getStoreFiles();
    }

    public synchronized List<StoreFile> getStoreFiles(int i) {
        return editVersion.getStoreFiles(i);
    }


    // 读操作函数
    public abstract KeyValue get(Key key);
    public abstract KeyValueIterator iterator(Key startKey, Key endKey);
}
