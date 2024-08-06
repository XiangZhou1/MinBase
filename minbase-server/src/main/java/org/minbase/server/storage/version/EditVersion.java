package org.minbase.server.storage.version;

import org.minbase.server.storage.store.StoreFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

// 读可以在历史版本进度读
// 写需要再最新版本上写
public class EditVersion {
    private static final Logger logger = LoggerFactory.getLogger(EditVersion.class);

    EditVersion prevVersion;
    AtomicLong readReference;

    FileEdit fileEdit;
    private SortedMap<Integer, List<StoreFile>> storeFiles;

    public EditVersion() {
        storeFiles = new ConcurrentSkipListMap<>();
        readReference = new AtomicLong();
    }

    public EditVersion(EditVersion editVersion) {
        this.storeFiles = new ConcurrentSkipListMap<>();
        readReference = new AtomicLong();

        final Iterator<Map.Entry<Integer, List<StoreFile>>> iterator = editVersion.storeFiles.entrySet().iterator();
        while (iterator.hasNext()) {
            final Map.Entry<Integer, List<StoreFile>> entry = iterator.next();
            this.storeFiles.put(entry.getKey(), new ArrayList<>(entry.getValue()));
        }
    }

    public List<StoreFile> getStoreFiles(int level) {
        return this.storeFiles.computeIfAbsent(level, integer -> new LinkedList<>());
    }

    public SortedMap<Integer, List<StoreFile>> getStoreFiles() {
        return storeFiles;
    }

    public synchronized EditVersion applyFileEdit(FileEdit fileEdit) {
        EditVersion editVersion = new EditVersion(this);
        editVersion.applyFileEditImpl(fileEdit);
        editVersion.setPrevVersion(this);
        this.fileEdit = fileEdit;
        return editVersion;
    }

    public void setPrevVersion(EditVersion prevVersion) {
        this.prevVersion = prevVersion;
    }

    public EditVersion getPrevVersion() {
        return prevVersion;
    }

    public void deleteFile() {
        //
        Set<String> addedSSTableIdSet = new HashSet<>();
        SortedMap<Integer, List<StoreFile>> addedSSTables = fileEdit.getAddedSSTables();
        for (Map.Entry<Integer, List<StoreFile>> entry : addedSSTables.entrySet()) {
            Integer level = entry.getKey();
            List<StoreFile> addedSSTablesOneLevel = entry.getValue();
            List<StoreFile> storeFiles = getStoreFiles(level);
            for (StoreFile storeFile : addedSSTablesOneLevel) {
                storeFiles.add(storeFile);
                addedSSTableIdSet.add(storeFile.getStoreId());
            }
        }
        // 真正删除文件
        SortedMap<Integer, List<StoreFile>> removedSSTables = fileEdit.getRemovedSSTables();
        for (Map.Entry<Integer, List<StoreFile>> entry : removedSSTables.entrySet()) {
            List<StoreFile> removedSSTablesOneLevel = entry.getValue();
            for (StoreFile storeFile : removedSSTablesOneLevel) {
                if (!addedSSTableIdSet.contains(storeFile.getStoreId())) {
                    File file = new File(storeFile.getFilePath());
                    logger.info("Delete old file, fileName=" + file.getName());
                    if (!file.delete()) {
                        logger.info("Delete old file fail, fileName=" + file.getName());
                    }
                }
            }
        }
    }

    private void applyFileEditImpl(FileEdit fileEdit) {
        SortedMap<Integer, List<StoreFile>> addedSSTables = fileEdit.getAddedSSTables();
        for (Map.Entry<Integer, List<StoreFile>> entry : addedSSTables.entrySet()) {
            Integer level = entry.getKey();
            List<StoreFile> addedSSTablesOneLevel = entry.getValue();
            List<StoreFile> storeFiles = getStoreFiles(level);
            for (StoreFile storeFile : addedSSTablesOneLevel) {
                storeFiles.add(storeFile);
            }
        }

        SortedMap<Integer, List<StoreFile>> removedSSTables = fileEdit.getRemovedSSTables();
        for (Map.Entry<Integer, List<StoreFile>> entry : removedSSTables.entrySet()) {
            Integer level = entry.getKey();
            List<StoreFile> removedSSTablesOneLevel = entry.getValue();
            List<StoreFile> storeFiles = getStoreFiles(level);
            for (StoreFile storeFile : removedSSTablesOneLevel) {
                storeFiles.remove(storeFile);
            }
        }
    }

    public void acquireReadReference() {
        this.readReference.incrementAndGet();
    }

    public void releaseReadReference() {
        this.readReference.decrementAndGet();
    }

    public long getReadReference() {
        return readReference.get();
    }
}
