package org.minbase.server.kv.store;


import org.minbase.server.kv.compaction.CompactThread;
import org.minbase.server.kv.compaction.Compaction;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.Value;
import org.minbase.server.kv.WriteBatch;

import org.minbase.server.conf.Configuration;
import org.minbase.server.constant.Constants;
import org.minbase.server.kv.iterator.KeyValueIterator;
import org.minbase.server.kv.iterator.MemStoreIterator;
import org.minbase.server.kv.iterator.MergeIterator;
import org.minbase.server.kv.compaction.CompactionStrategy;
import org.minbase.server.kv.storage.storefilemanager.AbstractStoreFileManager;
import org.minbase.server.kv.storage.storefilemanager.level.LevelStoreFileManager;
import org.minbase.server.kv.storage.storefilemanager.tiered.TieredStoreFileManager;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Store {
    private String name;
    private File dir;

    Configuration configuration;
    /**
     * 内存存储
     */
    private MemStore memStore;
    /**
     * 冻结的内存存储
     */
    private ConcurrentLinkedDeque<MemStore> freezedMemStores;

    // 文件存储
    private AbstractStoreFileManager storeFileManager;
    private ReentrantReadWriteLock rwLock;
    private ReentrantReadWriteLock.WriteLock writeLock;
    private ReentrantReadWriteLock.ReadLock readLock;

    /**
     * 文件刷写线程
     */
    private Executor flushThread;

    /**
     * 文件压缩线程
     */
    private Compaction compaction;
    private CompactThread compactThread;

    public Store(String name, File dir, Configuration conf,
                 Executor flushThread, Compaction compaction,
                 CompactThread compactThread) throws IOException {
        this.name = name;
        this.dir = dir;
        this.configuration = conf;
        this.flushThread = flushThread;

        this.memStore = new MemStore(conf);
        this.freezedMemStores = new ConcurrentLinkedDeque<MemStore>();

        this.rwLock = new ReentrantReadWriteLock();
        this.writeLock = rwLock.writeLock();
        this.readLock = rwLock.readLock();
        this.compaction = compaction;
        this.compactThread = compactThread;

        initStoreManager();
    }

    private void initStoreManager() throws IOException {
        String compactionStrategy = Configuration.get(Constants.KEY_COMPACTION_STRATEGY);
        if (CompactionStrategy.LEVEL_COMPACTION.toString().equals(compactionStrategy)) {
            this.storeFileManager = new LevelStoreFileManager();
            this.storeFileManager.loadStoreFiles();
        } else if (CompactionStrategy.TIERED_COMPACTION.toString().equals(compactionStrategy)) {
            this.storeFileManager = new TieredStoreFileManager();
            this.storeFileManager.loadStoreFiles();
        }
    }


    /**
     * put实现
     */
    public void put(Key key, Value value) {
        writeLock();
        try {
            memStore.put(key, value);
        } finally {
            writeUnLock();
        }
    }

    public void put(KeyValue keyValue) {
        put(keyValue.getKey(), keyValue.getValue());
    }

    public void put(WriteBatch writeBatch) {
        writeLock();
        try {
            for (KeyValue keyValue : writeBatch.getKeyValues(name)) {
                memStore.put(keyValue.getKey(), keyValue.getValue());
            }
        } finally {
            writeUnLock();
        }

        if (memStore.shouldFreeze()) {
            freezeMemStore();
        }
    }

    private void freezeMemStore() {
        writeLock();
        try {
            MemStore currentMemStore = this.memStore;
            if (!currentMemStore.shouldFreeze()) {
                return;
            }
            freezedMemStores.addFirst(currentMemStore);
            this.memStore = new MemStore(configuration);
        } finally {
            writeUnLock();
        }
        flushThread.execute(new FlushTask(this));
    }

    public KeyValueIterator iterator(Key startKey, Key endKey) {
        readLock();
        List<KeyValueIterator> result = new ArrayList<>();
        try {
            MemStoreIterator memStoreIterator = memStore.iterator(startKey, endKey);
            result.add(memStoreIterator);
            for (MemStore freezedMemStore : freezedMemStores) {
                MemStoreIterator iterator = freezedMemStore.iterator(startKey, endKey);
                result.add(iterator);
            }
            result.add(storeFileManager.iterator(startKey, endKey));
        } finally {
            readUnLock();
        }
        return new MergeIterator(result);
    }


    public KeyValueIterator iterator() {
        return iterator(null, null);
    }

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


    public AbstractStoreFileManager getStorageManager() {
        return storeFileManager;
    }

    public ConcurrentLinkedDeque<MemStore> getFreezedMemStores() {
        return freezedMemStores;
    }

    public void foreFlush() {
        freezeMemStore();
        final FlushTask flushTask = new FlushTask(this);
        while (!this.freezedMemStores.isEmpty()) {
            // 此处不能这样
            flushTask.flush();
        }
    }

    public void triggerCompaction() {
        if (compaction.needCompact(storeFileManager)) {
            compactThread.trigger();
        }
    }

}
