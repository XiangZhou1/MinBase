package org.minbase.server.lsmStorage;



import org.minbase.server.iterator.KeyIterator;
import org.minbase.server.iterator.MergeIterator;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.storage.sstable.SSTable;
import org.minbase.server.utils.ByteUtils;
import org.minbase.server.version.EditVersion;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class LevelStorageManager extends StorageManager {
    private List<SSTable> l0SSTables;
    private SortedMap<Integer, List<SSTable>> levelTables;
    private LsmStorage lsmStorage;


    public LevelStorageManager(LsmStorage lsmStorage) {
        l0SSTables = new CopyOnWriteArrayList<>();
        levelTables = new TreeMap<>();
        this.lsmStorage = lsmStorage;
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
                    getSSTables(level).add(ssTable);
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
        SSTable ssTable = new SSTable();
        ssTable.setSsTableId(ssTableId);
        ssTable.setFilePath(getFilePath(ssTableId));
        try (RandomAccessFile ssTableFile = getSSTableFile(ssTableId, "r")) {
            ssTable.loadFromFile(ssTableFile);
        }
        return ssTable;
    }

    @Override
    public KeyValue get(Key key) {
        ArrayList<KeyIterator> list = new ArrayList<>();
        for (SSTable l0SSTable : l0SSTables) {
            if(l0SSTable.mightContain(key.getUserKey())){
                list.add(l0SSTable.iterator(key,null));
            }
        }

        for (List<SSTable> ssTables : levelTables.values()) {
            for (SSTable ssTable : ssTables) {
                if(ssTable.mightContain(key.getUserKey())){
                    list.add(ssTable.iterator(key,null));
                }
            }
        }

        MergeIterator mergeIterator = new MergeIterator(list);
        mergeIterator.seek(key);

        if (mergeIterator.isValid()) {
            if (ByteUtils.byteEqual(key.getUserKey(), mergeIterator.key().getUserKey())){
                return mergeIterator.value();
            }
        }
        return null;
    }


    @Override
    public void addNewSSTable(SSTable ssTable, long lastSyncSequenceId) throws IOException{
        ssTable.setSsTableId(UUID.randomUUID().toString());
        // 存到
        saveSSTableFile(ssTable);
        getSSTables(0).add(ssTable);
        this.lastSequenceId = lastSyncSequenceId;
        saveManifest();
    }

    @Override
    public KeyIterator iterator(Key startKey, Key endKey) {
        ArrayList<KeyIterator> list = new ArrayList<>();
        for (SSTable l0SSTable : l0SSTables) {
            if(l0SSTable.inRange(startKey, endKey)){
                list.add(l0SSTable.iterator(startKey, endKey));
            }
        }

        for (List<SSTable> ssTables : levelTables.values()) {
            for (SSTable ssTable : ssTables) {
                if (ssTable.inRange(startKey, endKey)) {
                    list.add(ssTable.iterator(startKey, endKey));
                }
            }
        }

        return new MergeIterator(list);
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


    public List<SSTable> getSSTables(int level) {
        return level == 0 ? this.l0SSTables : this.levelTables.computeIfAbsent(level, integer -> new LinkedList<>());
    }

    @Override
    public EditVersion newEditVersion() {
        return new EditVersion();
    }

    @Override
    public synchronized void applyEditVersion(EditVersion editVersion) {
        lsmStorage.writeLock();
        try {
            //
            SortedMap<Integer, List<SSTable>> removedSSTables = editVersion.getRemovedSSTables();
            for (Map.Entry<Integer, List<SSTable>> entry : removedSSTables.entrySet()) {
                Integer level = entry.getKey();
                List<SSTable> removedSSTablesOneLevel = entry.getValue();
                List<SSTable> ssTables = getSSTables(level);
                for (SSTable ssTable : removedSSTablesOneLevel) {
                    ssTables.remove(ssTable);
                }
            }

            SortedMap<Integer, List<SSTable>> addedSSTables = editVersion.getAddedSSTables();
            for (Map.Entry<Integer, List<SSTable>> entry : addedSSTables.entrySet()) {
                Integer level = entry.getKey();
                List<SSTable> addedSSTablesOneLevel = entry.getValue();
                List<SSTable> ssTables = level == 0 ? this.l0SSTables : this.levelTables.get(level);
                for (SSTable ssTable : addedSSTablesOneLevel) {
                    ssTables.add(ssTable);
                    System.out.println("add " + ssTable.getSsTableId());
                }

            }

            //
            saveManifest();

        } finally {
            lsmStorage.writeUnLock();
        }

        // 真正删除文件
        SortedMap<Integer, List<SSTable>> removedSSTables = editVersion.getRemovedSSTables();
        for (Map.Entry<Integer, List<SSTable>> entry : removedSSTables.entrySet()) {
            List<SSTable> removedSSTablesOneLevel = entry.getValue();
            for (SSTable ssTable : removedSSTablesOneLevel) {
                File file = new File(ssTable.getFilePath());
                if(!file.delete()){
                    System.out.println("delete file fail, " + ssTable.getFilePath());
                }
            }
        }
    }


    @Override
    public synchronized void saveManifest() {
        try {
            try (FileOutputStream outputStream = new FileOutputStream(getManiFestFile())) {
                outputStream.write(("lastSequenceId:" + lastSequenceId + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
                outputStream.write(("0:" + l0SSTables.size() + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
                for (SSTable l0SSTable : l0SSTables) {
                    outputStream.write(l0SSTable.getSsTableId().getBytes(StandardCharsets.UTF_8));
                    outputStream.write(" ".getBytes(StandardCharsets.UTF_8));
                }
                outputStream.write(System.lineSeparator().getBytes(StandardCharsets.UTF_8));

                for (Map.Entry<Integer, List<SSTable>> entry : levelTables.entrySet()) {
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
}
