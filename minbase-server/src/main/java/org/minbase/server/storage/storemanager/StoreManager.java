package org.minbase.server.storage.storemanager;

import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.storage.store.StoreFile;
import org.minbase.server.storage.version.FileEdit;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.SortedMap;

public interface StoreManager extends ManiFest {
    File getStoreDir();

    long getLastSequenceId();

    // SSTable 操作
    void loadStoreFiles() throws IOException;

    void addStoreFile(StoreFile storeFile, long lastSyncSequenceId) throws IOException;

    // 读操作
    KeyValue get(Key key);

    KeyValueIterator iterator(Key startKey, Key endKey);
}
