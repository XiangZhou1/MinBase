package org.minbase.server.lsmStorage;



import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.storage.sstable.SSTable;
import org.minbase.server.storage.edit.FileEdit;

import java.io.IOException;

public class SimpleStorageManager extends StorageManager {

    @Override
    public void loadSSTables() {

    }

    @Override
    public KeyValue get(Key key) {
        return null;
    }


    @Override
    public void addNewSSTable(SSTable ssTable, long lastSyncSequenceId) throws IOException {

    }

    @Override
    public FileEdit newFileEdit() {
        return null;
    }

    @Override
    public void applyFileEdit(FileEdit fileEdit) {

    }

    @Override
    public KeyValueIterator iterator(Key startKey, Key endKey) {
        return null;
    }

    @Override
    public void saveManifest() {

    }

    @Override
    public void saveSSTableFile(SSTable ssTable) throws IOException {

    }
}
