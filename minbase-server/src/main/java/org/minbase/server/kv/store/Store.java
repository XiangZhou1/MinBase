package org.minbase.server.kv.store;


import org.minbase.common.utils.ByteUtil;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.Value;

import org.minbase.server.conf.Configuration;
import org.minbase.server.kv.iterator.KeyValueIterator;
import org.minbase.server.kv.storage.StoreFileManager;
import org.minbase.server.kv.utils.KeyUtil;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Store {
    private String name;
    private File storeDir;

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
    private StoreFileManager storeFileManager;
    private ReentrantReadWriteLock rwLock;
    private ReentrantReadWriteLock.WriteLock writeLock;
    private ReentrantReadWriteLock.ReadLock readLock;

    /**
     * 文件刷写线程
     */
    private ThreadPoolExecutor flushThreadPool;
    /**
     * 上次刷写的序列号
     */
    private long flushedSequenceId;
    private ReentrantLock flushLock;
    private StoreManager storeManager;

    public Store(String name, File storDir, Configuration conf,
                 ThreadPoolExecutor flushThreadPool, StoreManager storeManager) throws IOException {
        this.name = name;
        this.storeDir = storDir;
        this.configuration = conf;
        this.flushThreadPool = flushThreadPool;

        this.memStore = new MemStore(conf);
        this.freezedMemStores = new ConcurrentLinkedDeque<MemStore>();

        this.rwLock = new ReentrantReadWriteLock();
        this.writeLock = rwLock.writeLock();
        this.readLock = rwLock.readLock();
        this.flushLock = new ReentrantLock();
        this.storeManager = storeManager;
        this.storeFileManager = new StoreFileManager(storeDir, this, conf);
    }

    public Store(String storeName, File storDir, Configuration configuration, StoreManager storeManager)
            throws IOException {
        this(storeName, storDir, configuration, new ThreadPoolExecutor(1, 1,
                1, TimeUnit.MINUTES, new ArrayBlockingQueue<>(1000),
                new ThreadPoolExecutor.AbortPolicy()), storeManager);
    }

    /**
     * put实现
     */
    public void put(Key key, Value value) {
        put(new KeyValue(key, value));
    }

    public void put(KeyValue keyValue) {
        writeLock();
        try {
            memStore.put(keyValue);
        } finally {
            writeUnLock();
        }

        if (memStore.shouldFreeze()) {
            freezeMemStore();
        }
    }

    public void put(List<KeyValue> keyValues) {
        writeLock();
        try {
            for (KeyValue keyValue : keyValues) {
                memStore.put(keyValue);
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
        flushThreadPool.execute(new FlushTask(this, storeManager));
    }

    public KeyValueIterator iterator(Key startKey, Key endKey) {
        return new StoreIterator(this, startKey, endKey);
    }

    /**
     * 获取key对应的value
     *
     * @param key 如果key的version为LATEST_VERSION, 则返回最新版本的value
     *            否则返回指定版本的value
     * @return value
     */
    public KeyValue get(Key key) {
        Key startKey = null;
        Key endKey = null;
        if (key.isLatestVersion()) {
            startKey = KeyUtil.latestVersionKey(key.getInternalKey());
            endKey = KeyUtil.earliestVersionKey(key.getInternalKey());
        } else {
            startKey = key;
            endKey = KeyUtil.earliestVersionKey(key.getInternalKey());
        }
        StoreIterator storeIterator = new StoreIterator(this, startKey, endKey);
        while (storeIterator.hasNext()) {
            storeIterator.next();
            KeyValue keyValue = storeIterator.value();
            if (keyValue == null) {
                return null;
            }
            if (ByteUtil.ByteEqual(keyValue.getKey().getInternalKey(), key.getInternalKey())) {
                return keyValue;
            }
        }
        return null;
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

    public StoreFileManager getStorageManager() {
        return storeFileManager;
    }

    public ConcurrentLinkedDeque<MemStore> getFreezedMemStores() {
        return freezedMemStores;
    }

    public void foreFlush() {
        writeLock();
        try {
            MemStore currentMemStore = this.memStore;
            freezedMemStores.addFirst(currentMemStore);
            this.memStore = new MemStore(configuration);
        } finally {
            writeUnLock();
        }
        final FlushTask flushTask = new FlushTask(this, storeManager);
        while (!this.freezedMemStores.isEmpty()) {
            flushTask.flush();
        }
    }

    public void triggerCompaction() {
        this.storeFileManager.requestCompaction();
    }

    public void setFlushedSequenceId(long lastSyncSequenceId) {
        this.flushedSequenceId = lastSyncSequenceId;
    }

    public long getFlushedSequenceId() {
        return flushedSequenceId;
    }


    public boolean flushLock() {
        return flushLock.tryLock();
    }

    public void flushUnLock() {
        flushLock.unlock();
    }

    public String getName() {
        return name;
    }

    public MemStore getMemStore() {
        return memStore;
    }


    public StoreFileManager getStoreFileManager() {
        return storeFileManager;
    }

    public StoreManager getStoreManager() {
        return storeManager;
    }
}
