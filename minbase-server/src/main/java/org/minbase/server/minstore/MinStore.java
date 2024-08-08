package org.minbase.server.minstore;


import org.minbase.common.operation.Delete;
import org.minbase.common.operation.Get;
import org.minbase.common.operation.Put;
import org.minbase.server.compaction.CompactThread;
import org.minbase.server.compaction.Compaction;
import org.minbase.server.op.*;
import org.minbase.server.storage.storemanager.level.LevelStoreManager;
import org.minbase.server.storage.storemanager.tiered.TieredStoreManager;
import org.minbase.server.conf.Config;
import org.minbase.server.constant.Constants;
import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.iterator.MemStoreIterator;
import org.minbase.server.iterator.MergeIterator;
import org.minbase.server.mem.MemStore;
import org.minbase.server.compaction.CompactionStrategy;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.storage.storemanager.AbstractStoreManager;
import org.minbase.server.wal.LogEntry;
import org.minbase.server.wal.Wal;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MinStore {
    private static final int MAX_IMMEMTABLE_SIZE = 3;
    private String name;
    private File dir;

    // 内存存储结构
    private MemStore memStore;
    private ConcurrentLinkedDeque<MemStore> immMemStores;
    // 文件存储
    private AbstractStoreManager storeManager;

    private ReentrantReadWriteLock rwLock;
    private ReentrantReadWriteLock.WriteLock writeLock;
    private ReentrantReadWriteLock.ReadLock readLock;

    // 文件刷写线程
    private Executor flushThread;
    // 文件压缩线程
    private Compaction compaction;
    private CompactThread compactThread;

    public MinStore(String name, File dir, Executor flushThread, Compaction compaction, CompactThread compactThread) throws IOException {
        this.name = name;
        this.dir = dir;
        this.flushThread = flushThread;

        this.memStore = new MemStore();
        this.immMemStores = new ConcurrentLinkedDeque<MemStore>();

        this.rwLock = new ReentrantReadWriteLock();
        this.writeLock = rwLock.writeLock();
        this.readLock = rwLock.readLock();
        this.compaction = compaction;
        this.compactThread = compactThread;

        initStoreManager();
    }

    private void initStoreManager() throws IOException {
        String compactionStrategy = Config.get(Constants.KEY_COMPACTION_STRATEGY);
        if (CompactionStrategy.LEVEL_COMPACTION.toString().equals(compactionStrategy)) {
            this.storeManager = new LevelStoreManager();
            this.storeManager.loadStoreFiles();
        } else if (CompactionStrategy.TIERED_COMPACTION.toString().equals(compactionStrategy)) {
            this.storeManager = new TieredStoreManager();
            this.storeManager.loadStoreFiles();
        }
    }

    // ======================================================
    // get函数, 拿到最新值
    public KeyValue get(Get get) {
        final byte[] userKey = get.getKey();
        final KeyValueIterator iterator = iterator(Key.minKey(userKey), Key.maxKey(userKey));
        try {
            RowTacker tacker = new RowTacker(Key.latestKey(get.getKey()), new HashSet<>(get.getColumns()));
            while (iterator.isValid()) {
                final KeyValue keyValue = iterator.value();
                tacker.track(keyValue);
                if (tacker.shouldStop()) {
                    return tacker.getKeyValue();
                }
                iterator.nextInnerKey();
            }
            return tacker.getKeyValue();
        } finally {
            iterator.close();
        }
    }


    //===========================
    // put实现
    public void put(Put put) {
        WriteBatch writeBatch = new WriteBatch();
        for (Map.Entry<byte[], byte[]> entry : put.getColumnValues().entrySet()) {
            Key key = new Key(put.getKey(), Constants.NO_VERSION);
            Value value = Value.Put();
            value.addColumnValue(entry.getKey(), entry.getValue());
            writeBatch.add(new KeyValue(key, value));
        }

        put(writeBatch);
    }

    public void put(WriteBatch writeBatch) {
        for (KeyValue keyValue : writeBatch.getKeyValues()) {
            memStore.put(keyValue.getKey(), keyValue.getValue());
        }
        if (memStore.shouldFreeze()) {
            freezeMemTable();
        }
    }

    public boolean checkAndPut(byte[] checkKey, byte[] column, byte[] checkValue, Put put) {
        writeLock();
        try {
            Get get = new Get(checkKey);
            get.addColumn(column);

            KeyValue keyValue = get(get);
            byte[] oldValue = keyValue.getValue().getColumnValues().get(column);
            if ((oldValue == null && checkValue == null) || ByteUtil.byteEqual(oldValue, checkValue)) {
                put(put);
                return true;
            } else {
                return false;
            }
        } finally {
            writeUnLock();
        }
    }

    private void freezeMemTable() {
        MemStore currentMemStore = this.memStore;
        writeLock();
        try {
            if (this.memStore != currentMemStore) {
                return;
            }
            immMemStores.addFirst(currentMemStore);
            this.memStore = new MemStore();
            // 进行刷写文件
            if (immMemStores.size() >= MAX_IMMEMTABLE_SIZE) {
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
        memStore.put(kv.getKey(), kv.getValue());
        if (memStore.shouldFreeze()) {
            freezeMemTable();
        }
    }

    public void delete(Delete delete) {
        WriteBatch writeBatch = new WriteBatch();
        final List<byte[]> columns = delete.getColumns();
        if (columns.isEmpty()) {
            Key key = new Key(delete.getKey(), Constants.NO_VERSION);
            Value value = Value.Delete();
            writeBatch.add(new KeyValue(key, value));
        } else {
            for (byte[] column : columns) {
                Key key = new Key(delete.getKey(), Constants.NO_VERSION);
                Value value = Value.DeleteColumn(column);
                writeBatch.add(new KeyValue(key, value));
            }
        }
        this.put(writeBatch);
    }

    // ===================================================================
    public KeyValueIterator iterator(Key startKey, Key endKey) {
        ArrayList<KeyValueIterator> result = new ArrayList<>();
        MemStoreIterator iterator1 = memStore.iterator(startKey, endKey);
        result.add(iterator1);

        ArrayList<KeyValueIterator> list1 = new ArrayList<>();
        for (MemStore immMemStore : immMemStores) {
            MemStoreIterator iterator = immMemStore.iterator(startKey, endKey);
            list1.add(iterator);
        }
        result.add(new MergeIterator(list1));
        result.add(storeManager.iterator(startKey, endKey));
        return new MergeIterator(result);
    }


    public KeyValueIterator iterator() {
        return iterator(null, null);
    }

    public KeyValueIterator scan(byte[] start, byte[] end) {
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


    public AbstractStoreManager getStorageManager() {
        return storeManager;
    }

    public ConcurrentLinkedDeque<MemStore> getImmMemTables() {
        return immMemStores;
    }

    public void applyWal(LogEntry logEntry) {
        for (KeyValue keyValue : logEntry.getKeyValues()) {
            memStore.put(keyValue.getKey(), keyValue.getValue());
        }
        if (this.memStore.shouldFreeze()) {
            freezeMemTable();
        }
    }

    public void foreFlush() {
        freezeMemTable();
        final FlushTask flushTask = new FlushTask(this);
        while (!this.immMemStores.isEmpty()) {
            // 此处不能这样
            flushTask.flush();
        }
    }

    public void triggerCompaction() {
        if (compaction.needCompact(storeManager)) {
            compactThread.trigger();
        }
    }

}
