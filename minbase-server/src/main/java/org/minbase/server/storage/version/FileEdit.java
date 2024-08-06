package org.minbase.server.storage.version;



import org.minbase.server.storage.store.StoreFile;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;

public class FileEdit {
    private SortedMap<Integer, List<StoreFile>> removedSSTables;
    private SortedMap<Integer, List<StoreFile>> addedSSTables;

    public FileEdit() {
        removedSSTables = new TreeMap<>();
        addedSSTables = new TreeMap<>();
    }

    public synchronized void removeSSTable(int level, StoreFile storeFile) {
        List<StoreFile> storeFiles = removedSSTables.computeIfAbsent(level, new Function<Integer, List<StoreFile>>() {
            @Override
            public List<StoreFile> apply(Integer integer) {
                return new ArrayList<StoreFile>();
            }
        });

        storeFiles.add(storeFile);
    }

    public synchronized void addSSTable(int level, StoreFile storeFile) {
        List<StoreFile> storeFiles = addedSSTables.computeIfAbsent(level, new Function<Integer, List<StoreFile>>() {
            @Override
            public List<StoreFile> apply(Integer integer) {
                return new ArrayList<StoreFile>();
            }
        });
        storeFiles.add(storeFile);
    }

    public SortedMap<Integer, List<StoreFile>> getRemovedSSTables() {
        return removedSSTables;
    }

    public SortedMap<Integer, List<StoreFile>> getAddedSSTables() {
        return addedSSTables;
    }
}
