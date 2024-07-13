package org.minbase.server.compaction.level;


import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.iterator.MergeIterator;
import org.minbase.server.iterator.StorageIterator;
import org.minbase.server.lsmStorage.StorageManager;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.storage.edit.EditVersion;
import org.minbase.server.storage.edit.FileEdit;
import org.minbase.server.storage.sstable.SSTable;
import org.minbase.common.utils.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LevelStorageManager extends StorageManager {
    private static final Logger logger = LoggerFactory.getLogger(LevelStorageManager.class);

    private volatile EditVersion editVersion;
    private Thread clearOldVersionThread;
    public LevelStorageManager() {
        editVersion = new EditVersion();
        clearOldVersionThread = new Thread(new ClearOldVersionTask(), "ClearOldVersionThread");
        clearOldVersionThread.start();
    }

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
        return Data_Dir + File.separator + "level" + File.separator + ssTableId;
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
    public KeyValue get(Key key) {
        ArrayList<KeyValueIterator> list = new ArrayList<>();
//        for (SSTable l0SSTable : editVersion.getSSTables(0)) {
//            if(l0SSTable.mightContain(key.getUserKey())){
//                list.add(l0SSTable.iterator(key,null));
//            }
//        }

        EditVersion currentVersion = getReadEditVersion();
        try {
            for (List<SSTable> ssTables : currentVersion.getLevelTables().values()) {
                for (SSTable ssTable : ssTables) {
                    if (ssTable.mightContain(key.getUserKey())) {
                        list.add(ssTable.iterator(key, null));
                    }
                }
            }

            MergeIterator mergeIterator = new MergeIterator(list);
            mergeIterator.seek(key);

            if (mergeIterator.isValid()) {
                if (ByteUtil.byteEqual(key.getUserKey(), mergeIterator.key().getUserKey())) {
                    return mergeIterator.value();
                }
            }
            return null;
        } finally {
            currentVersion.releaseReadReference();
        }
    }


    @Override
    public void addNewSSTable(SSTable ssTable, long lastSyncSequenceId) throws IOException{
        // 存到
        saveSSTableFile(ssTable);
        final FileEdit fileEdit = newFileEdit();
        fileEdit.addSSTable(0, ssTable);
        applyFileEdit(fileEdit);
        this.lastSequenceId = lastSyncSequenceId;
    }



    @Override
    public KeyValueIterator iterator(Key startKey, Key endKey) {
        ArrayList<KeyValueIterator> list = new ArrayList<>();
//        for (SSTable l0SSTable : l0SSTables) {
//            if(l0SSTable.inRange(startKey == null ? null : startKey.getUserKey(), endKey == null ? null : endKey.getUserKey(), false)){
//                list.add(l0SSTable.iterator(startKey, endKey));
//            }
//        }
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

    public void saveSSTableFile(SSTable ssTable) throws IOException {
        File dir = new File( Data_Dir + File.separator + "level" );
        if (!dir.exists()) {
            dir.mkdirs();
        }
        ssTable.setFilePath(getFilePath(ssTable.getSsTableId()));
        try (RandomAccessFile ssTableFile = getSSTableFile(ssTable.getSsTableId(), "rw")) {
            ssTableFile.write(ssTable.encode());
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
    
    public synchronized EditVersion getReadEditVersion() {
        this.editVersion.acquireReadReference();
        return this.editVersion;
    }

    @Override
    public synchronized void saveManifest() {
        System.out.println("saveMan");
        try {
            try (FileOutputStream outputStream = new FileOutputStream(getManiFestFile())) {
                outputStream.write(("lastSequenceId:" + lastSequenceId + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
//                outputStream.write(("0:" + l0SSTables.size() + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
//                for (SSTable l0SSTable : l0SSTables) {
//                    outputStream.write(l0SSTable.getSsTableId().getBytes(StandardCharsets.UTF_8));
//                    outputStream.write(" ".getBytes(StandardCharsets.UTF_8));
//                }
//                outputStream.write(System.lineSeparator().getBytes(StandardCharsets.UTF_8));

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
            System.exit(-1);
        }
    }

    public List<SSTable> getSSTables(int i) {
        return this.editVersion.getSSTables(i);
    }


    private class ClearOldVersionTask implements Runnable {
        @Override
        public void run() {
            while (true) {
                EditVersion currentVersion = editVersion;
                EditVersion removeVersion = currentVersion.getPrevVersion();
                if (removeVersion == null) {
                    try {
                        Thread.sleep(10 * 1000);
                    } catch (InterruptedException interruptedException) {
                        interruptedException.printStackTrace();
                    }
                    continue;
                }
                while (removeVersion != null) {
                    if (removeVersion.getReadReference() == 0) {
                        logger.info("Clear old version, delete file");
                        removeVersion.deleteFile();
                        currentVersion.setPrevVersion(removeVersion.getPrevVersion());
                    }
                    currentVersion = removeVersion;
                    removeVersion = currentVersion.getPrevVersion();
                }
            }
        }
    }
}
