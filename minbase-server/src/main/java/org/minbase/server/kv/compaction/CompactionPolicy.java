package org.minbase.server.kv.compaction;

import org.minbase.server.kv.storage.StoreFile;
import org.minbase.server.kv.storage.StoreFileManager;

import java.util.List;

public interface CompactionPolicy {
    CompactionResult compact(StoreFileManager storeFileManager, List<StoreFile> filesToCompact, long minReadPoint) throws Exception;

    boolean shouldCompact(StoreFileManager storeFileManager, List<StoreFile> filesToCompact);
}
