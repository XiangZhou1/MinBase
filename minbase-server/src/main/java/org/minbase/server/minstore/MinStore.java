package org.minbase.server.minstore;


import org.minbase.server.compaction.CompactThread;
import org.minbase.server.compaction.Compaction;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.Value;
import org.minbase.server.storage.storemanager.level.LevelStoreManager;
import org.minbase.server.storage.storemanager.tiered.TieredStoreManager;
import org.minbase.server.conf.Config;
import org.minbase.server.constant.Constants;
import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.iterator.MemStoreIterator;
import org.minbase.server.iterator.MergeIterator;
import org.minbase.server.mem.MemStore;
import org.minbase.server.compaction.CompactionStrategy;
import org.minbase.server.storage.storemanager.StoreManager;
import org.minbase.server.utils.ValueUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MinStore {
    private static final int MAX_IMMEMTABLE_SIZE = 3;
    private String name;
    private File sotreDir;

    // 内存存储结构
    private MemStore memStore;
    private ConcurrentLinkedDeque<MemStore> immMemStores;
    // 文件存储
    private StoreManager storeManager;

    private ReentrantReadWriteLock rwLock;
    private ReentrantReadWriteLock.WriteLock writeLock;
    private ReentrantReadWriteLock.ReadLock readLock;

    // 文件刷写线程
    private Executor flushThread;
    // 文件压缩线程
    private Compaction compaction;
    private CompactThread compactThread;

    public MinStore(String name, File sotreDir, Executor flushThread, Compaction compaction, CompactThread compactThread) throws IOException {
        this.name = name;
        this.sotreDir = sotreDir;
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
            this.storeManager = new LevelStoreManager(this.sotreDir);
            this.storeManager.loadStoreFiles();
        } else if (CompactionStrategy.TIERED_COMPACTION.toString().equals(compactionStrategy)) {
            this.storeManager = new TieredStoreManager(this.sotreDir);
            this.storeManager.loadStoreFiles();
        }
    }

    // ======================================================
    // get函数, 拿到最新值
    public KeyValue get(Key key) {
        readLock();
        try {
            KeyValue keyValue;
            keyValue = memStore.get(key);
            if (keyValue != null) {
                return keyValue;
            }

            for (MemStore immMemStore : immMemStores) {
                keyValue = immMemStore.get(key);
                if (keyValue != null) {
                    return keyValue;
                }
            }

            keyValue = storeManager.get(key);
            return keyValue;
        } finally {
            readUnLock();
        }
    }



    //===========================
    // put实现
    public void put(Key key, Value value) {
        memStore.put(key, value);
    }


    //===========================
    // delete实现
    public void delete(Key key) {
        KeyValue kv = new KeyValue(key, ValueUtils.Delete());
        memStore.put(kv.getKey(), kv.getValue());
        if (memStore.shouldFreeze()) {
            freezeMemTable();
        }
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


    public StoreManager getStorageManager() {
        return storeManager;
    }

    public ConcurrentLinkedDeque<MemStore> getImmMemTables() {
        return immMemStores;
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
