package org.minbase.server.kv.compaction;

import org.minbase.server.kv.storage.StoreFile;
import org.minbase.server.kv.storage.StoreFileManager;
import org.minbase.server.kv.store.StoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CompactionChecker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(CompactionChecker.class);
    StoreFileManager storeFileManager;
    private CompactionPolicy compactionPolicy;
    private Thread currentThread = null;
    private StoreManager storeManager;
    public Object waitLock = new Object();
    private ExecutorService compactExecutorService = Executors.newSingleThreadExecutor();

    public CompactionChecker(StoreManager storeManager, StoreFileManager storeFileManager) {
        this.storeFileManager = storeFileManager;
        this.storeManager = storeManager;
        this.compactionPolicy = storeFileManager.getCompactionPolicy();
    }

    @Override
    public void run() {
        currentThread = Thread.currentThread();
        while (true) {
            try {
                List<StoreFile> storeFilesToCompact = storeFileManager.getStoreFilesToCompact();
                if (compactionPolicy.shouldCompact(storeFileManager, storeFilesToCompact)) {
                    LOG.info("StoreFilesToCompact:{}", storeFilesToCompact.size());
                    CompactionResult compactionResult = compactionPolicy.compact(storeFileManager,
                            storeFilesToCompact, storeManager.getMinReadPointOfScanner());
                    if (compactionResult != null) {
                        applyConpactionResult(compactionResult);
                    }
                }
                synchronized (waitLock) {
                    waitLock.wait(10 * 1000);
                }
            } catch (Exception e) {
                LOG.error("Compaction error", e);
            }
        }
    }

    public void requestCompaction() {
        synchronized (waitLock) {
            waitLock.notify();
        }
    }

    private void applyConpactionResult(CompactionResult compactionResult) {
        if (compactionResult.isEmpty()) {
            return;
        }
        storeFileManager.updateStoreFiles(compactionResult.getFilesToAdd(), compactionResult.getFilesToDelete());
        storeFileManager.updateStoreFilesIterators(compactionResult.getFilesToDelete());

        // 实际删除
        storeFileManager.deleteStoreFiles(compactionResult.getFilesToDelete());
    }

    public void start() {
        compactExecutorService.execute(this);
    }
}
