package org.minbase.server.lsmStorage;



import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.storage.sstable.SSTable;
import org.minbase.server.storage.version.EditVersion;
import org.minbase.server.storage.version.FileEdit;

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
    public void addSSTable(SSTable ssTable, long lastSyncSequenceId) throws IOException {

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
    public EditVersion getEditVersion() {
        return null;
    }
}
