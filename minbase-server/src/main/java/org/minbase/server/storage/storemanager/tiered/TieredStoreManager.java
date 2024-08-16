package org.minbase.server.storage.storemanager.tiered;


import org.minbase.common.utils.ByteUtil;
import org.minbase.server.compaction.CompactionStrategy;
import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.iterator.MergeIterator;
import org.minbase.server.iterator.StoreIterator;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.storage.store.StoreFile;
import org.minbase.server.storage.storemanager.StoreManager;
import org.minbase.server.storage.version.EditVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TieredStoreManager extends StoreManager {
    private static final Logger logger = LoggerFactory.getLogger(TieredStoreManager.class);

    public TieredStoreManager() {
        super();
        this.compactionStrategy = CompactionStrategy.TIERED_COMPACTION;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // 读写操作
    @Override
    public KeyValue get(Key key) {
        EditVersion currentVersion = getEditVersion(true);
        try {
            ArrayList<KeyValueIterator> list = new ArrayList<>();
            for (List<StoreFile> storeFiles : currentVersion.getStoreFiles().values()) {
                for (StoreFile storeFile : storeFiles) {
                    if (storeFile.mightContain(key.getUserKey())) {
                        list.add(storeFile.getReader().iterator(key, null));
                    }
                }
            }

            MergeIterator mergeIterator = new MergeIterator(list);
            mergeIterator.seek(key);

            if (mergeIterator.isValid()) {
                if (ByteUtil.byteEqual(key.getUserKey(), mergeIterator.key().getUserKey())) {
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
                if (storeFile.inRange(startKey == null ? null : startKey.getUserKey(), endKey == null ? null : endKey.getUserKey(), false)) {
                    list.add(storeFile.getReader().iterator(startKey, endKey));
                }
            }
        }
        return new StoreIterator(list, currentVersion);
    }


}
