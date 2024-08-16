package org.minbase.server.storage.version;

import org.minbase.common.utils.Util;
import org.minbase.server.storage.storemanager.StoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClearOldVersionTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ClearOldVersionTask.class);

    StoreManager storeManager;

    public ClearOldVersionTask(StoreManager storeManager) {
        this.storeManager = storeManager;
    }

    @Override
    public void run() {
        while (true) {
            EditVersion currentVersion = storeManager.getEditVersion(false);
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
