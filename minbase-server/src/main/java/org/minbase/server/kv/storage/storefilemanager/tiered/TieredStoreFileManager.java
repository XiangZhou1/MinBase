package org.minbase.server.kv.storage.storefilemanager.tiered;


import org.minbase.common.utils.ByteUtil;
import org.minbase.server.kv.compaction.CompactionStrategy;
import org.minbase.server.kv.iterator.KeyValueIterator;
import org.minbase.server.kv.iterator.MergeIterator;
import org.minbase.server.kv.store.StoreIterator;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.storage.storefile.StoreFile;
import org.minbase.server.kv.storage.storefilemanager.AbstractStoreFileManager;
import org.minbase.server.kv.storage.version.EditVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TieredStoreFileManager extends AbstractStoreFileManager {
    private static final Logger logger = LoggerFactory.getLogger(TieredStoreFileManager.class);

    public TieredStoreFileManager() {
        super();
        this.compactionStrategy = CompactionStrategy.TIERED_COMPACTION;
    }

    /// ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // 读写操作
    @Override
    public KeyValue get(Key key) {
        EditVersion currentVersion = getEditVersion(true);
        try {
            ArrayList<KeyValueIterator> list = new ArrayList<>();
            for (List<StoreFile> storeFiles : currentVersion.getStoreFiles().values()) {
                for (StoreFile storeFile : storeFiles) {
                    if (storeFile.mightContain(key.getInternalKey())) {
                        list.add(storeFile.getReader().iterator(key, null));
                    }
                }
            }

            MergeIterator mergeIterator = new MergeIterator(list);
            mergeIterator.seek(key);

            if (mergeIterator.hasNext()) {
                if (ByteUtil.byteEqual(key.getInternalKey(), mergeIterator.key().getInternalKey())) {
                    return mergeIterator.value();
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
