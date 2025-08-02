package org.minbase.server.storage.storefilemanager;

import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.storage.storefile.StoreFile;

import java.io.File;
import java.io.IOException;

public interface StoreFileManager extends ManiFest {
    File getStoreDir();

    long getLastSequenceId();

    // SSTable 操作
    void loadStoreFiles() throws IOException;

    void addStoreFile(StoreFile storeFile, long lastSyncSequenceId) throws IOException;

    // 读操作
    KeyValue get(Key key);

    KeyValueIterator iterator(Key startKey, Key endKey);
}
