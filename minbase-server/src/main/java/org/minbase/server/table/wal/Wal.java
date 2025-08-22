package org.minbase.server.table.wal;


import org.minbase.common.utils.Util;
import org.minbase.server.conf.Configuration;
import org.minbase.server.constant.Constants;
import org.minbase.server.kv.WriteBatch;
import org.minbase.common.utils.ByteUtil;
import org.minbase.common.utils.FileUtil;
import org.minbase.server.kv.store.StoreManager;
import org.minbase.server.table.TableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.LockSupport;

public class Wal {
    private static final Logger LOG = LoggerFactory.getLogger(Wal.class);

    private static final String WAL_DIR = Configuration.get(Constants.KEY_DATA_DIR) + File.separator + "wal";
    public static final int WAL_NUM_LIMIT = 10000;
    public static final long WAL_FILE_LENGTH_LIMIT = Util.parseUnit(Configuration.get(Constants.KEY_WAL_FILE_LENGTH_LIMIT));
    private static final SyncLevel syncLevel = SyncLevel.valueOf(Configuration.get(Constants.KEY_WAL_SYNC_LEVEL));
    private static final String INPROGRESS_WAL = "wal.inprogress";

    private File walDir;
    private File walFile;
    private FileOutputStream outputStream;
    // 已经记录到文件的序号
    private volatile long syncedLogSequenceId = 0;
    // 记录日志的队列
    private LinkedBlockingQueue<LogEntry> queue;
    // 同步日志到文件的线程
    private Thread syncWalThread;
    private SyncWalTask syncWalTask;
    private ConcurrentSkipListMap<Long, Thread> waitingSyncThreads = new ConcurrentSkipListMap<>();
    private TableManager tableManager;

    public Wal(File walDir, TableManager tableManager) throws IOException {
        this.walDir = walDir;
        this.tableManager = tableManager;
        if (!walDir.exists()) {
            if (!walDir.mkdirs()) {
                throw new IOException("Create wal dir fail, dir=" + walDir);
            }
        }
        this.queue = new LinkedBlockingQueue<>();
        this.syncWalTask = new SyncWalTask();
        this.syncWalThread = new Thread(syncWalTask, "SyncWalTask");
        this.syncWalThread.start();
    }

    /**
     * 原子性记录多条日志
     */
    public void log(WriteBatch writeBatch) {
        LogEntry logEntry = new LogEntry(writeBatch);
        log(logEntry);
    }

    public void log(LogEntry logEntry) {
        try {
            queue.put(logEntry);
            trySyncWal(logEntry.getLastSequenceId());
        } catch (InterruptedException e) {
            LOG.error("Log fail, logEntry=" + logEntry, e);
        }
    }

    private void trySyncWal(long currSequenceId) {
        if (syncedLogSequenceId >= currSequenceId) {
            return;
        }
        if (SyncLevel.SYNC.equals(syncLevel)) {
            while (syncedLogSequenceId < currSequenceId) {
                waitingSyncThreads.put(currSequenceId, Thread.currentThread());
                LockSupport.park();
            }
        }
    }

    /**
     * 从日志文件中恢复日志
     */
    public synchronized void recovery() throws IOException {
        final File[] files = listWalFiles();
        long lastSequenceId = 0;
        if (files != null && files.length != 0) {
            for (File file1 : files) {
                recoveryFromFile(file1);
            }
        }
        File inProgressFile = new File(walDir, INPROGRESS_WAL);
        if (inProgressFile.exists()) {
            long[] sequenceIds = recoveryFromFile(inProgressFile);
            FileUtil.rename(inProgressFile, new File(walDir, sequenceIds[0] + "_" + sequenceIds[1]));
        }
        LOG.info("Wal recovery, lastSequenceId=" + lastSequenceId);
    }

    private long[] recoveryFromFile(File file1) throws IOException {
        LogEntry logEntry = null;
        long firstSequenceId = -1;
        long lastSequenceId = -1;
        try (RandomAccessFile raf = new RandomAccessFile(file1, "r")) {
            int pos = 0;
            while (pos < raf.length()) {
                byte[] buf = FileUtil.read(raf, Constants.INTEGER_LENGTH);
                int logEntryLength = ByteUtil.byteArrayToInt(buf, 0);
                pos += Constants.INTEGER_LENGTH;

                byte[] logEntryBuf = FileUtil.read(raf, logEntryLength);
                logEntry = new LogEntry();
                logEntry.decode(logEntryBuf);
                pos += logEntryLength;
                tableManager.applyLog(logEntry);
                if (firstSequenceId == -1) {
                    firstSequenceId = logEntry.getFirstSequenceId();
                }
                lastSequenceId = logEntry.getLastSequenceId();
            }
        }
        return new long[]{firstSequenceId, lastSequenceId};
    }

    private File[] listWalFiles() {
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

    public boolean shouldForeFlush() {
        return false;
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
                            startId = logEntry.getFirstSequenceId();
                            walFileLength = 0;
                        }
                        outputStream.write(ByteUtil.intToByteArray(logEntry.length()));
                        outputStream.write(logEntry.encode());
                        outputStream.flush();
                        outputStream.getChannel().force(true);
                        syncedLogSequenceId = logEntry.getLastSequenceId();
                        walFileLength += logEntry.length() + Constants.INTEGER_LENGTH;
                        if (shouldCloseFile()) {
                            closeFile();
                        }
                        wakeWaitingSyncThreads();
                    } catch (Exception e) {
                        LOG.error("Sync log entry fail, logEntry=" + (logEntry == null ? "null" : logEntry));
                    }
                }
            }
        }

        private boolean shouldCloseFile() {
            return syncedLogSequenceId - startId > WAL_NUM_LIMIT || walFileLength >= WAL_FILE_LENGTH_LIMIT;
        }


        private void openFile() throws FileNotFoundException {
            walFile = new File(walDir, INPROGRESS_WAL);
            outputStream = new FileOutputStream(walFile);
        }

        private void closeFile() throws IOException {
            outputStream.getChannel().force(true);
            outputStream.close();
            File file = new File(walDir, startId + "_" + syncedLogSequenceId);
            FileUtil.rename(walFile, file);
            LOG.info("Flush wal file, fileName=" + file.getName());
            walFile = null;
            startId = -1;
            walFileLength = 0;
        }

        private void wakeWaitingSyncThreads(){
            Map.Entry<Long, Thread> entry;
            while ((entry = waitingSyncThreads.firstEntry()) != null) {
                if (entry.getKey() <= syncedLogSequenceId) {
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
                LOG.info("Clear old wal, fileName=" + file1.getName());
            } else {
                break;
            }
        }
    }
}
