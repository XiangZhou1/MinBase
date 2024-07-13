package org.minbase.server.lsmStorage;


import org.minbase.server.conf.Config;
import org.minbase.server.constant.Constants;
import org.minbase.server.iterator.KeyIterator;
import org.minbase.server.iterator.MemTableIterator;
import org.minbase.server.iterator.MergeIterator;
import org.minbase.server.iterator.SnapshotIterator;
import org.minbase.server.mem.MemTable;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.op.Value;
import org.minbase.server.op.WriteBatch;
import org.minbase.server.storage.compaction.CompactThread;
import org.minbase.server.storage.compaction.Compaction;
import org.minbase.server.storage.compaction.CompactionStrategy;
import org.minbase.server.storage.compaction.LevelCompaction;
import org.minbase.common.utils.ByteUtils;
import org.minbase.server.wal.LogEntry;
import org.minbase.server.wal.Wal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LsmStorage {
    private static final int MAX_IMMEMTABLE_SIZE = 3;
    // 内存存储结构
    private MemTable memTable;
    private ConcurrentLinkedDeque<MemTable> immMemTables;
    // 文件存储
    private StorageManager storageManager;

    // 文件刷写线程
    private Executor flushThread;
    
    // 文件压缩线程
    private Compaction compaction;
    private CompactThread compactThread;

    private ReentrantReadWriteLock rwLock;
    private ReentrantReadWriteLock.WriteLock writeLock;
    private ReentrantReadWriteLock.ReadLock readLock;

    private Wal wal;

    public LsmStorage() throws IOException {
        this.memTable =new MemTable();
        this.immMemTables = new ConcurrentLinkedDeque<MemTable>();

        this.rwLock = new ReentrantReadWriteLock();
        this.writeLock = rwLock.writeLock();
        this.readLock = rwLock.readLock();

        initStorageManager();

        // 刷写线程
        flushThread = Executors.newSingleThreadExecutor();
        // 压缩线程
        this.compactThread.start();
        // wal 日志
        wal = new Wal(storageManager.lastSequenceId);
        wal.recovery(this);
    }

    private void initStorageManager() throws IOException {
        String compactionStrategy = Config.get(Constants.KEY_COMPACTION_STRATEGY);
        if (CompactionStrategy.LEVEL_COMPACTION.toString().equals(compactionStrategy)) {
            this.storageManager = new LevelStorageManager();
            this.storageManager.loadSSTables();
            this.compaction = new LevelCompaction((LevelStorageManager)storageManager);
        } else {

        }
        this.compactThread = new CompactThread(this.compaction);
    }
    
    // ======================================================
    // get函数, 拿到最新值
    public byte[] get(byte[] userKey) {
        KeyValue kv = getInner(Key.latestKey(userKey));
        if (kv == null) {
            return null;
        }
        Value value = kv.getValue();
        if (value != null) {
            return value.isDeleteOP() ? null : value.value();
        }
        return null;
    }

    public byte[] get(Key key) {
        KeyValue kv = getInner(key);
        if (kv == null) {
            return null;
        }
        Value value = kv.getValue();
        if (value != null) {
            return value.isDeleteOP() ? null : value.value();
        }
        return null;
    }

    public KeyValue getInner(Key key) {
        readLock();
        try {
            KeyValue keyValue;
            keyValue = memTable.get(key);
            if (keyValue != null) {
                return keyValue;
            }

            for (MemTable immMemTable : immMemTables) {
                keyValue = immMemTable.get(key);
                if (keyValue != null) {
                    return keyValue;
                }
            }

            keyValue = storageManager.get(key);
            return keyValue;
        } finally {
            readUnLock();
        }
    }

    
    //===========================
    // put实现
    public void put(byte[] key, byte[] value) {
        KeyValue kv = new KeyValue(new Key(key, 0), Value.Put(value));
        wal.log(kv);
        memTable.put(kv.getKey(), kv.getValue());
        if (memTable.shouldFreeze()) {
            freezeMemTable();
        }
    }

    public void put(WriteBatch writeBatch) {
        wal.log(writeBatch);
        for (KeyValue keyValue : writeBatch.getKeyValues()) {
            memTable.put(keyValue.getKey(), keyValue.getValue());
        }
        if (memTable.shouldFreeze()) {
            freezeMemTable();
        }
    }


    public boolean checkAndPut(byte[] checkKey, byte[] checkValue, byte[] key, byte[] value) {
        writeLock();
        try {
            final byte[] bytes = get(checkKey);
            if (!ByteUtils.byteEqual(bytes, checkValue)) {
                return false;
            }
            put(key, value);
            return true;
        } finally {
            writeUnLock();
        }
    }

    private void freezeMemTable() {
        MemTable currentMemTable = this.memTable;
        writeLock();
        try {
            if (this.memTable != currentMemTable) {
                return;
            }
            immMemTables.addFirst(currentMemTable);
            this.memTable = new MemTable();
            // 进行刷写文件
            if (immMemTables.size() >= MAX_IMMEMTABLE_SIZE) {
                // 此处不能这样
                flushThread.execute(new FlushTask(this));
            }
        } finally {
            writeUnLock();
        }
    }


    //===========================
    // delete实现
    public void delete(byte[] key) {
        KeyValue kv = new KeyValue(new Key(key, 0), Value.Delete());
        wal.log(kv);
        memTable.put(kv.getKey(), kv.getValue());
        if (memTable.shouldFreeze()) {
            freezeMemTable();
        }
    }

    
    // ===================================================================
    public KeyIterator iterator(Key startKey, Key endKey) {
        ArrayList<KeyIterator> result = new ArrayList<>();
        MemTableIterator iterator1 = memTable.iterator(startKey, endKey);
        result.add(iterator1);

        ArrayList<KeyIterator> list1 = new ArrayList<>();
        for (MemTable immMemTable : immMemTables) {
            MemTableIterator iterator = immMemTable.iterator(startKey, endKey);
            list1.add(iterator);
        }
        result.add(new MergeIterator(list1));
        result.add(storageManager.iterator(startKey, endKey));
        return new MergeIterator(result);
    }



    public KeyIterator iterator() {
        return iterator(null, null);
    }
    
    public SnapshotIterator scan(byte[] start, byte[] end, long snapshot) {
        return new SnapshotIterator(iterator(Key.minKey(start), Key.maxKey(end)), snapshot) ;
    }

    public KeyIterator scan(byte[] start, byte[] end) {
        return iterator(Key.minKey(start), Key.maxKey(end));
    }

    
    
    // ==================================================
    // 其余辅助函数
    public void readLock() {
        this.readLock.lock();
    }

    public void readUnLock() {
        this.readLock.unlock();
    }

    public void writeLock() {
        this.writeLock.lock();
    }

    public void writeUnLock() {
        this.writeLock.unlock();
    }

    public void triggerCompaction() {
        if (compaction.shouldCompact()) {
            compactThread.trigger();
        }
    }

    public void clearOldWal(long syncSequenceId) {
        wal.clearOldWal(syncSequenceId);
    }

    public StorageManager getStorageManager() {
        return storageManager;
    }

    public ConcurrentLinkedDeque<MemTable> getImmMemTables() {
        return immMemTables;
    }

    public void applyWal(LogEntry logEntry) {
        for (KeyValue keyValue : logEntry.getKeyValues()) {
            memTable.put(keyValue.getKey(), keyValue.getValue());
        }
        if (this.memTable.shouldFreeze()) {
            freezeMemTable();
        }
    }

    public long getSnapshot() {
        return wal.getSequenceId();
    }

    public void foreFlush(){
        freezeMemTable();
        final FlushTask flushTask = new FlushTask(this);
        while (!this.immMemTables.isEmpty()){
            // 此处不能这样
            flushTask.flush();
        }
    }


}
