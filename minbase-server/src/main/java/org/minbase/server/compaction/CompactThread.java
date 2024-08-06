package org.minbase.server.compaction;

import org.minbase.common.utils.Util;
import org.minbase.server.table.TableImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.*;

public class CompactThread implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(CompactThread.class);
    Map<String, TableImpl> tables;
    private ExecutorService compactThread;
    private Compaction compaction;
    private Thread currentThread = null;

    public CompactThread(Compaction compaction, Map<String, TableImpl> tables) {
        this.compaction = compaction;
        this.compactThread = Executors.newCachedThreadPool();
        this.tables = tables;
    }

    @Override
    public void run() {
        currentThread = Thread.currentThread();
        while (true) {
            ArrayList<Future<?>> tasks = new ArrayList<>();
            for (TableImpl table : tables.values()) {
                try {
                    if (compaction.needCompact(table.getMinStore().getStorageManager())) {
                        Future<?> task = compactThread.submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    compaction.compact(table.getMinStore().getStorageManager());
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        });
                        tasks.add(task);
                    }
                } catch (Exception e) {
                    logger.error("Compaction error", e);
                }
            }
            for (Future<?> task : tasks) {
                try {
                    task.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (tasks.isEmpty()) {
                Util.sleep(60 * 1000);
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
