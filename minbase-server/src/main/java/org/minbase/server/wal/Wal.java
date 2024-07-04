package org.minbase.server.wal;


import org.minbase.server.conf.Config;
import org.minbase.server.constant.Constants;
import org.minbase.server.lsmStorage.LsmStorage;
import org.minbase.server.op.KeyValue;
import org.minbase.server.op.WriteBatch;
import org.minbase.server.utils.ByteUtils;
import org.minbase.server.utils.IOUtils;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.LockSupport;

public class Wal {
    private static final String Wal_Dir = Config.get(Constants.KEY_DATA_DIR) + File.separator + "wal";
    public static final int MAX_WAL_NUM = 10000;
    public static final int MAX_WAL_FILE_LENGTH = 10 * 1024 * 1024;
    private static SyncLevel syncLevel = SyncLevel.valueOf(Config.get(Constants.KEY_WAL_SYNC_LEVEL));
    private static String INPROGRESS_WAL = "wal.inprogress";
    private File file;
    private OutputStream outputStream;
    private long sequenceId = 0;
    private Queue<LogEntry> queue;

    private Thread syncWalThread;
    private volatile long syncSequenceId = 0;
    private ConcurrentSkipListMap<Long, Thread> waitingSyncThreads = new ConcurrentSkipListMap<>();

    public Wal(long sequenceId) {
        final File walDir = new File(Wal_Dir);
        if (!walDir.exists()) {
            walDir.mkdirs();
        }
        this.sequenceId = this.syncSequenceId = sequenceId;
        queue = new ConcurrentLinkedDeque<>();
        syncWalThread = new Thread(new SyncWalTask());
        syncWalThread.start();
    }

    public  void log (KeyValue keyValue) {
        long currSequenceId;
        synchronized (this){
            currSequenceId = sequenceId ++;
            LogEntry logEntry = new LogEntry(currSequenceId, Arrays.asList(keyValue));
            queue.offer(logEntry);
        }
       trySyncWal(currSequenceId);
    }

    public void log(WriteBatch writeBatch) {
        long currSequenceId;
        synchronized (this){
            currSequenceId = sequenceId ++;
            writeBatch.getKeyValues().forEach( keyValue -> keyValue.getKey().setSequenceId(currSequenceId));
            LogEntry logEntry = new LogEntry(currSequenceId, writeBatch.getKeyValues());
            queue.offer(logEntry);
        }
        trySyncWal(currSequenceId);
    }

    private void trySyncWal(long currSequenceId) {
        if (syncSequenceId >= currSequenceId) {
            return;
        }
        LockSupport.unpark(syncWalThread);

        if (SyncLevel.SYNC.equals(syncLevel)) {
            while (syncSequenceId < currSequenceId) {
                waitingSyncThreads.put(currSequenceId, Thread.currentThread());
                LockSupport.park();
            }
        }
    }

    public synchronized void recovery(LsmStorage lsmStorage) throws IOException {
        final File[] files = listSyncWals();
        if (files == null) {
            return;
        }
        for (File file1 : files) {
            recoveryFromFile(lsmStorage, file1);
        }

        File inProgressFile = new File(Wal_Dir + File.separator + INPROGRESS_WAL);
        if (inProgressFile.exists()) {
            recoveryFromFile(lsmStorage, inProgressFile);
        }

    }

    private void recoveryFromFile(LsmStorage lsmStorageInner, File file1) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file1, "r")) {
            byte[] buf = IOUtils.read(raf, Constants.INTEGER_LENGTH);
            int pos = 0;

            while (pos < buf.length) {
                int logEntryLength = ByteUtils.byteArrayToInt(buf, 0);
                pos += Constants.INTEGER_LENGTH;

                byte[] logEntryBuf = IOUtils.read(raf, logEntryLength);
                LogEntry logEntry = new LogEntry();
                logEntry.decode(logEntryBuf);
                pos += logEntryLength;

                lsmStorageInner.applyWal(logEntry);
                sequenceId = logEntry.getSequenceId();
            }
        }
    }

    private File[] listSyncWals() {
        File walDir = new File(Wal_Dir);
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
     * 每1wang
     */
    private class SyncWalTask implements  Runnable {
        long startId = -1;
        long walLength = 0;
        @Override
        public void run() {
            while (true) {
                try {
                    while (queue.isEmpty()) {
                        LockSupport.park();
                    }
                    final LogEntry logEntry = queue.poll();
                    if (startId == -1) {
                        openFile();
                        startId = logEntry.getSequenceId();
                        walLength = 0;
                    }

                    outputStream.write(logEntry.encode());
                    outputStream.flush();
                    syncSequenceId = logEntry.getSequenceId();
                    walLength += logEntry.length();

                    if (shouldCloseFile()) {
                        closeFile();
                    }
                    wakeWaitingSyncThreads();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        private boolean shouldCloseFile() {
            return syncSequenceId - startId > MAX_WAL_NUM || walLength >= MAX_WAL_FILE_LENGTH;
        }


        private void openFile() throws FileNotFoundException {
            file = new File(Wal_Dir + File.separator + INPROGRESS_WAL);
            outputStream = new FileOutputStream(file);
        }

        private void closeFile() throws IOException {
            outputStream.close();
            file.renameTo(new File(Wal_Dir + File.separator + startId + "_" + syncSequenceId));
            file = null;
            startId = -1;
            walLength = 0;
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


    public void clearOldWal(long oldSequenceId) {
        final File[] files = listSyncWals();
        for (File file1 : files) {
            long syncId = Long.parseLong(file1.getName().split("_")[1]);
            if (syncId <= oldSequenceId) {
                file1.delete();
            } else {
                break;
            }
        }
    }
}
