package org.minbase.server.storage.edit;



import org.minbase.server.storage.sstable.SSTable;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;

public class FileEdit {
    private SortedMap<Integer, List<SSTable>> removedSSTables;
    private SortedMap<Integer, List<SSTable>> addedSSTables;

    public FileEdit() {
        removedSSTables = new TreeMap<>();
        addedSSTables = new TreeMap<>();
    }

    public synchronized void removeSSTable(int level, SSTable ssTable){
        List<SSTable> ssTables = removedSSTables.computeIfAbsent(level, new Function<Integer, List<SSTable>>() {
            @Override
            public List<SSTable> apply(Integer integer) {
                return new ArrayList<SSTable>();
            }
        });

        ssTables.add(ssTable);
    }

    public synchronized void addSSTable(int level, SSTable ssTable){
        List<SSTable> ssTables = addedSSTables.computeIfAbsent(level, new Function<Integer, List<SSTable>>() {
            @Override
            public List<SSTable> apply(Integer integer) {
                return new ArrayList<SSTable>();
            }
        });
        ssTables.add(ssTable);
    }

    public SortedMap<Integer, List<SSTable>> getRemovedSSTables() {
        return removedSSTables;
    }

    public SortedMap<Integer, List<SSTable>> getAddedSSTables() {
        return addedSSTables;
    }
}
