package org.minbase.server.storage.edit;

import org.minbase.server.storage.sstable.SSTable;
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
    private SortedMap<Integer, List<SSTable>> levelTables;

    public EditVersion() {
        levelTables = new ConcurrentSkipListMap<>();
        readReference = new AtomicLong();
    }

    public EditVersion(EditVersion editVersion) {
        this.levelTables = new ConcurrentSkipListMap<>();
        readReference = new AtomicLong();

        final Iterator<Map.Entry<Integer, List<SSTable>>> iterator = editVersion.levelTables.entrySet().iterator();
        while (iterator.hasNext()) {
            final Map.Entry<Integer, List<SSTable>> entry = iterator.next();
            this.levelTables.put(entry.getKey(), new ArrayList<>(entry.getValue()));
        }
    }

    public List<SSTable> getSSTables(int level) {
        return this.levelTables.computeIfAbsent(level, integer -> new LinkedList<>());
    }

    public SortedMap<Integer, List<SSTable>> getLevelTables() {
        return levelTables;
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
        SortedMap<Integer, List<SSTable>> addedSSTables = fileEdit.getAddedSSTables();
        for (Map.Entry<Integer, List<SSTable>> entry : addedSSTables.entrySet()) {
            Integer level = entry.getKey();
            List<SSTable> addedSSTablesOneLevel = entry.getValue();
            List<SSTable> ssTables = getSSTables(level);
            for (SSTable ssTable : addedSSTablesOneLevel) {
                ssTables.add(ssTable);
                addedSSTableIdSet.add(ssTable.getSsTableId());
            }
        }
        // 真正删除文件
        SortedMap<Integer, List<SSTable>> removedSSTables = fileEdit.getRemovedSSTables();
        for (Map.Entry<Integer, List<SSTable>> entry : removedSSTables.entrySet()) {
            List<SSTable> removedSSTablesOneLevel = entry.getValue();
            for (SSTable ssTable : removedSSTablesOneLevel) {
                if (!addedSSTableIdSet.contains(ssTable.getSsTableId())) {
                    File file = new File(ssTable.getFilePath());
                    logger.info("Delete old file, fileName=" + file.getName());
                    if (!file.delete()) {
                        logger.info("Delete old file fail, fileName=" + file.getName());
                    }
                }
            }
        }
    }

    private void applyFileEditImpl(FileEdit fileEdit) {
        SortedMap<Integer, List<SSTable>> addedSSTables = fileEdit.getAddedSSTables();
        for (Map.Entry<Integer, List<SSTable>> entry : addedSSTables.entrySet()) {
            Integer level = entry.getKey();
            List<SSTable> addedSSTablesOneLevel = entry.getValue();
            List<SSTable> ssTables = getSSTables(level);
            for (SSTable ssTable : addedSSTablesOneLevel) {
                ssTables.add(ssTable);
            }
        }

        SortedMap<Integer, List<SSTable>> removedSSTables = fileEdit.getRemovedSSTables();
        for (Map.Entry<Integer, List<SSTable>> entry : removedSSTables.entrySet()) {
            Integer level = entry.getKey();
            List<SSTable> removedSSTablesOneLevel = entry.getValue();
            List<SSTable> ssTables = getSSTables(level);
            for (SSTable ssTable : removedSSTablesOneLevel) {
                ssTables.remove(ssTable);
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
