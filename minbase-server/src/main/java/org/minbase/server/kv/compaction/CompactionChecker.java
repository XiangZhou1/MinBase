package org.minbase.server.kv.compaction;

import org.minbase.server.constant.Constants;
import org.minbase.server.kv.storage.StoreFile;
import org.minbase.server.kv.storage.StoreFileManager;
import org.minbase.server.kv.store.StoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CompactionChecker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(CompactionChecker.class);
    StoreFileManager storeFileManager;
    private CompactionPolicy compactionPolicy;
    private StoreManager storeManager;
    public Object waitLock = new Object();
    private ExecutorService compactExecutorService = Executors.newSingleThreadExecutor();
    private boolean stop = false;
    public CompactionChecker(StoreManager storeManager, StoreFileManager storeFileManager) {
        this.storeFileManager = storeFileManager;
        this.storeManager = storeManager;
        this.compactionPolicy = storeFileManager.getCompactionPolicy();
    }

    public void setStop(boolean stop) {
        this.stop = stop;
    }

    @Override
    public void run() {
        long compactCheckInterval = this.storeManager.getConfiguration().getLong(Constants.COMPACT_CHECK_INTERVAL_KEY, Constants.COMPACT_CHECK_INTERVAL_DEFAULT);
        while (!stop) {
            try {
                synchronized (waitLock) {
                    waitLock.wait(compactCheckInterval);
                }
                if (stop) {
                    return;
                }
                compact();
            } catch (Exception e) {
                LOG.error("Compaction error", e);
            }
        }
    }

    public CompactionResult compact() throws Exception {
        CompactionResult compactionResult = null;
        List<StoreFile> storeFilesToCompact = storeFileManager.getStoreFilesToCompact();
        if (compactionPolicy.shouldCompact(storeFileManager, storeFilesToCompact)) {
            long minReadPoint = storeManager.getMinReadPoint();
            compactionResult = compactionPolicy.compact(storeFileManager,
                    storeFilesToCompact, minReadPoint);
            if (compactionResult != null) {
                applyConpactionResult(compactionResult);
                LOG.info("Compact file, minReadPoint:{}, selectedFileSize:{}, addedFileSize:{}", minReadPoint, compactionResult.getFilesToDelete().size(),
                        compactionResult.getFilesToAdd().size());
            }
        }
        return compactionResult;
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
        // 实际删除
        storeFileManager.deleteStoreFiles(compactionResult.getFilesToDelete());
    }

    public void start() {
        compactExecutorService.execute(this);
    }

    public void close () throws InterruptedException {
        setStop(true);
        requestCompaction();
        compactExecutorService.shutdownNow();
        compactExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
    }
}
