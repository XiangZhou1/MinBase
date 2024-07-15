package org.minbase.server.storage.version;

import org.minbase.common.utils.Util;
import org.minbase.server.compaction.level.LevelStorageManager;
import org.minbase.server.lsmStorage.StorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClearOldVersionTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ClearOldVersionTask.class);

    StorageManager storageManager;

    public ClearOldVersionTask(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    @Override
    public void run() {
        while (true) {
            EditVersion currentVersion = storageManager.getEditVersion();
            EditVersion removeVersion = currentVersion.getPrevVersion();
            if (removeVersion == null) {
                Util.sleep(10 * 1000);
                continue;
            }

            while (removeVersion != null) {
                if (removeVersion.getReadReference() == 0) {
                    logger.info("Clear old version, delete file");
                    removeVersion.deleteFile();
                    currentVersion.setPrevVersion(removeVersion.getPrevVersion());
                }
                currentVersion = removeVersion;
                removeVersion = currentVersion.getPrevVersion();
            }
        }
    }
}
