package org.minbase.server.wal;


import org.minbase.common.utils.Util;
import org.minbase.server.MinBaseServer;
import org.minbase.server.conf.Config;
import org.minbase.server.constant.Constants;
import org.minbase.server.minstore.MinStore;
import org.minbase.server.op.KeyValue;
import org.minbase.server.op.WriteBatch;
import org.minbase.common.utils.ByteUtil;
import org.minbase.common.utils.FileUtil;
import org.minbase.server.table.TableImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.LockSupport;

public class Wal {
    private static final Logger logger = LoggerFactory.getLogger(Wal.class);

    private static final String WAL_DIR = Config.get(Constants.KEY_DATA_DIR) + File.separator + "wal";
    public static final int WAL_NUM_LIMIT = 10000;
    public static final long WAL_FILE_LENGTH_LIMIT = Util.parseUnit(Config.get(Constants.KEY_WAL_FILE_LENGTH_LIMIT));
    private static final SyncLevel syncLevel = SyncLevel.valueOf(Config.get(Constants.KEY_WAL_SYNC_LEVEL));
    private static final String INPROGRESS_WAL = "wal.inprogress";

    private File walFile;
    private OutputStream outputStream;
    // 当前记录的日志序号
    private long sequenceId = 0;
    // 已经记录到文件的序号
    private volatile long syncSequenceId = 0;

    // 记录日志的队列
    private LinkedBlockingQueue<LogEntry> queue;

    // 同步日志到文件的线程
    private Thread syncWalThread;
    private SyncWalTask syncWalTask;
    private ConcurrentSkipListMap<Long, Thread> waitingSyncThreads = new ConcurrentSkipListMap<>();

    public Wal() {
    }

    public Wal(long sequenceId) {
        final File walDir = new File(WAL_DIR);
        if (!walDir.exists()) {
            walDir.mkdirs();
        }

        this.sequenceId = this.syncSequenceId = sequenceId;
        queue = new LinkedBlockingQueue<>();

        syncWalTask = new SyncWalTask();
        syncWalThread = new Thread(syncWalTask, "SyncWalTask");
        syncWalThread.start();
    }

    /**
     * 记录日志
     */
    public void log(KeyValue keyValue) {
        LogEntry logEntry = new LogEntry(keyValue);
        log(logEntry);
    }

    /**
     * 原子性记录多条日志
     */
    public void log(WriteBatch writeBatch) {
        LogEntry logEntry = new LogEntry(writeBatch.getKeyValues());
        log(logEntry);
    }

    private void log(LogEntry logEntry) {
        try {
            long currSequenceId;
            synchronized (this) {
                currSequenceId = sequenceId++;
                logEntry.setSequenceId(currSequenceId);
                queue.put(logEntry);
            }
            trySyncWal(currSequenceId);
        } catch (InterruptedException e) {
            logger.error("Log fail, logEntry=" + logEntry, e);
        }
    }

    private void trySyncWal(long currSequenceId) {
        if (syncSequenceId >= currSequenceId) {
            return;
        }

        if (SyncLevel.SYNC.equals(syncLevel)) {
            while (syncSequenceId < currSequenceId) {
                waitingSyncThreads.put(currSequenceId, Thread.currentThread());
                LockSupport.park();
            }
        }
    }

    /**
     * 从日志文件中恢复日志
     */
    public synchronized void recovery(ConcurrentHashMap<String, TableImpl> tables) throws IOException {
        final File[] files = listWalFiles();
        if (files == null) {
            return;
        }
        for (File file1 : files) {
            recoveryFromFile(tables, lastSequenceId, file1);
        }

        File inProgressFile = new File(WAL_DIR + File.separator + INPROGRESS_WAL);
        if (inProgressFile.exists()) {
            long startId = sequenceId;
            recoveryFromFile(minStore, lastSequenceId, inProgressFile);
            long endId = sequenceId;
            FileUtil.rename(inProgressFile, new File(WAL_DIR + File.separator + startId + "_" + endId));
        }
        logger.info("Wal recovery, sequenceId=" + sequenceId);
    }

    private void recoveryFromFile(ConcurrentHashMap<String, TableImpl> tables, long lastSequenceId, File file1) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file1, "r")) {
            int pos = 0;
            while (pos < raf.length()) {
                byte[] buf = FileUtil.read(raf, Constants.INTEGER_LENGTH);
                int logEntryLength = ByteUtil.byteArrayToInt(buf, 0);
                pos += Constants.INTEGER_LENGTH;

                byte[] logEntryBuf = FileUtil.read(raf, logEntryLength);
                LogEntry logEntry = new LogEntry();
                logEntry.decode(logEntryBuf);
                pos += logEntryLength;

                if (logEntry.getSequenceId() > lastSequenceId) {
                    minStoreInner.applyWal(logEntry);
                }
                sequenceId = syncSequenceId = logEntry.getSequenceId();
            }
        }
    }

    private File[] listWalFiles() {
        File walDir = new File(WAL_DIR);
        final File[] files = walDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return !pathname.getName().endsWith(INPROGRESS_WAL);
            }
        });
        if (files == null || files.length == 0) {
            return null;
        }
        Arrays.sort(files, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                long id1 = Long.parseLong(o1.getName().split("_")[1]);
                long id2 = Long.parseLong(o2.getName().split("_")[1]);
                return (int)(id1 - id2);
            }
        });
        return files;
    }

    public long getSequenceId() {
        return sequenceId;
    }


    /**
     * 记录日志到文件的线程
     */
    private class SyncWalTask implements  Runnable {
        long startId = -1;
        long walFileLength = 0;
        @Override
        public void run() {
            while (true) {
                LogEntry logEntry = null;
                synchronized (SyncWalTask.this) {
                    try {
                        logEntry = queue.take();
                        if (startId == -1) {
                            openFile();
                            startId = logEntry.getSequenceId();
                            walFileLength = 0;
                        }

                        outputStream.write(logEntry.encode());
                        outputStream.flush();
                        syncSequenceId = logEntry.getSequenceId();
                        walFileLength += logEntry.length();

                        if (shouldCloseFile()) {
                            closeFile();
                        }
                        wakeWaitingSyncThreads();
                    } catch (Exception e) {
                        logger.error("Sync log entry fail, logEntry=" + (logEntry == null ? "null" : logEntry));
                    }
                }
            }
        }

        private boolean shouldCloseFile() {
            return syncSequenceId - startId > WAL_NUM_LIMIT || walFileLength >= WAL_FILE_LENGTH_LIMIT;
        }


        private void openFile() throws FileNotFoundException {
            walFile = new File(WAL_DIR + File.separator + INPROGRESS_WAL);
            outputStream = new FileOutputStream(walFile);
        }

        private void closeFile() throws IOException {
            outputStream.close();
            File file = new File(WAL_DIR + File.separator + startId + "_" + syncSequenceId);
            FileUtil.rename(walFile, file);
            logger.info("Flush wal file, fileName=" + file.getName());
            walFile = null;
            startId = -1;
            walFileLength = 0;
        }

        private void wakeWaitingSyncThreads(){
            Map.Entry<Long, Thread> entry;
            while ((entry = waitingSyncThreads.firstEntry()) != null) {
                if (entry.getKey() <= syncSequenceId) {
                    LockSupport.unpark(entry.getValue());
                } else {
                    break;
                }
                waitingSyncThreads.remove(entry.getKey());
            }
        }
    }

    /**
     * 清理已经写入到SSTable文件中的日志
     *
     * @param oldSequenceId 记录到SSTable文件中的日志
     */
    public void clearOldWal(long oldSequenceId) {
        final File[] files = listWalFiles();
        if (files == null) {
            return;
        }
        for (File file1 : files) {
            long syncId = Long.parseLong(file1.getName().split("_")[1]);
            if (syncId <= oldSequenceId) {
                file1.delete();
                logger.info("Clear old wal, fileName=" + file1.getName());
            } else {
                break;
            }
        }
    }
}
