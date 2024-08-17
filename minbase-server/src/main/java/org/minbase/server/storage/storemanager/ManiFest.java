package org.minbase.server.storage.storemanager;


import org.minbase.common.utils.ByteUtil;
import org.minbase.common.utils.FileUtil;
import org.minbase.server.constant.Constants;
import org.minbase.server.storage.store.StoreFile;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

public class ManiFest {
    private static final String manifestFileName = "manifest";
    private StoreManager storeManager;
    private File maniFestFile;

    public ManiFest(StoreManager storeManager) {
        this.storeManager = storeManager;
        maniFestFile = new File(storeManager.getStoreDir(), manifestFileName);
    }

    /**
     * |sequenceId|level_size|level0_file_num|file1_name_size|fileName|........
     */
    public void saveManifest() throws IOException {
        FileOutputStream outputStream = new FileOutputStream(maniFestFile);
        encodeToFile(outputStream);
        outputStream.flush();
    }

    public void encodeToFile(OutputStream outputStream) throws IOException {
        outputStream.write(ByteUtil.longToByteArray(storeManager.getLastSequenceId()));

        SortedMap<Integer, List<StoreFile>> storeFiles = storeManager.getStoreFiles();
        outputStream.write(storeFiles.size());
        for (Map.Entry<Integer, List<StoreFile>> entry : storeFiles.entrySet()) {
            List<StoreFile> levelFiles = entry.getValue();
            outputStream.write(levelFiles.size());
            for (StoreFile file : levelFiles) {
                outputStream.write(file.getStoreId().length());
                outputStream.write(ByteUtil.toBytes(file.getStoreId()));
            }
        }
    }

    public void loadManiFest() throws IOException {
        byte[] buf = FileUtil.read(maniFestFile);
        int pos = 0;
        storeManager.lastSequenceId = ByteUtil.byteArrayToLong(buf, pos);
        pos += Constants.LONG_LENGTH;

        int levelNum = ByteUtil.byteArrayToInt(buf, pos);
        pos += Constants.INTEGER_LENGTH;
        for (int i = 0; i < levelNum; i++) {
            List<StoreFile> storeFiles = storeManager.getStoreFiles(i);
            int storeFilesNum = ByteUtil.byteArrayToInt(buf, pos);
            pos += Constants.INTEGER_LENGTH;

            for (int j = 0; j < storeFilesNum; j++) {
                int fileNameNum = ByteUtil.byteArrayToInt(buf, pos);
                pos += Constants.INTEGER_LENGTH;
                byte[] nameBytes = new byte[fileNameNum];
                System.arraycopy(buf, pos, nameBytes, 0, fileNameNum);
                String storeFileId = new String(nameBytes);
                storeFiles.add(new StoreFile(storeFileId));
            }
        }
    }

}
