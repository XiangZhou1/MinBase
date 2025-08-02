package org.minbase.server.storage.storefilemanager.level;


import org.minbase.common.utils.ByteUtil;
import org.minbase.server.compaction.CompactionStrategy;
import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.iterator.MergeIterator;
import org.minbase.server.iterator.StoreIterator;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.storage.storefile.StoreFile;
import org.minbase.server.storage.storefilemanager.AbstractStoreFileManager;
import org.minbase.server.storage.version.EditVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class LevelStoreFileManager extends AbstractStoreFileManager {
    private static final Logger logger = LoggerFactory.getLogger(LevelStoreFileManager.class);

    public LevelStoreFileManager() {
        super();
        this.compactionStrategy = CompactionStrategy.LEVEL_COMPACTION;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // 读写操作
    @Override
    public KeyValue get(Key key) {
        EditVersion currentVersion = getEditVersion(true);
        try {
            for (List<StoreFile> storeFiles : currentVersion.getStoreFiles().values()) {
                ArrayList<KeyValueIterator> list = new ArrayList<>();
                for (StoreFile storeFile : storeFiles) {
                    if (storeFile.mightContain(key.getInternalKey())) {
                        list.add(storeFile.getReader().iterator(key, null));
                    }
                }
                MergeIterator mergeIterator = new MergeIterator(list);
                mergeIterator.seek(key);

                if (mergeIterator.isValid()) {
                    if (ByteUtil.byteEqual(key.getInternalKey(), mergeIterator.key().getInternalKey())) {
                        return mergeIterator.value();
                    }
                }
            }
            return null;
        } finally {
            currentVersion.releaseReadReference();
        }
    }

    @Override
    public KeyValueIterator iterator(Key startKey, Key endKey) {
        ArrayList<KeyValueIterator> list = new ArrayList<>();
        EditVersion currentVersion = getEditVersion(true);
        for (List<StoreFile> storeFiles : currentVersion.getStoreFiles().values()) {
            for (StoreFile storeFile : storeFiles) {
                if (storeFile.inRange(startKey == null ? null : startKey.getInternalKey(), endKey == null ? null : endKey.getInternalKey(), false)) {
                    list.add(storeFile.getReader().iterator(startKey, endKey));
                }
            }
        }
        return new StoreIterator(list, currentVersion);
    }
}
