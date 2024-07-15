package org.minbase.server.compaction;

import org.minbase.common.utils.Util;
import org.minbase.server.compaction.level.LevelStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CompactThread implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(CompactThread.class);

    private ExecutorService compactThread;
    private Compaction compaction;
    private Thread currentThread = null;

    public CompactThread(Compaction compaction) {
        this.compaction = compaction;
        compactThread = Executors.newSingleThreadExecutor();
    }

    @Override
    public void run() {
        currentThread = Thread.currentThread();
        while (true) {
            try {
                if (!compaction.needCompact()) {
                    Util.sleep(60 * 1000);
                } else {
                    compaction.compact();
                }
            } catch (Exception e) {
                logger.error("Compaction error", e);
            }
        }
    }

    public void trigger() {
        currentThread.interrupt();
    }

    public void start() {
        compactThread.submit(this);
    }
}
