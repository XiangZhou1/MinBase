package org.minbase.server.storage.compaction;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CompactThread implements Runnable {
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
                if (!compaction.shouldCompact()) {
                    Thread.sleep(3 * 1000);
                } else {
                    compaction.compact();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void trigger(){
        currentThread.interrupt();
    }

    public void start() {
        compactThread.submit(this);
    }
}
