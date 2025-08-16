package org.minbase.server.kv.compaction;

import org.minbase.server.kv.storage.StoreFile;

import java.util.ArrayList;
import java.util.List;

public class CompactionResult {
    List<StoreFile> filesToDelete = new ArrayList<>();
    List<StoreFile> filesToAdd = new ArrayList<>();

    public void addFileToDelete(StoreFile storeFile) {
        filesToDelete.add(storeFile);
    }

    public void addFilesToDelete(List<StoreFile> storeFiles) {
        filesToDelete.addAll(storeFiles);
    }

    public void addFileToAdd(StoreFile storeFile) {
        filesToAdd.add(storeFile);
    }

    public boolean isEmpty() {
        return filesToDelete.isEmpty() && filesToAdd.isEmpty();
    }

    public List<StoreFile> getFilesToDelete() {
        return filesToDelete;
    }

    public List<StoreFile> getFilesToAdd() {
        return filesToAdd;
    }
}
