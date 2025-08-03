package org.minbase.server.kv.store;


import org.minbase.common.utils.ByteUtil;
import org.minbase.server.kv.compaction.CompactThread;
import org.minbase.server.kv.compaction.Compaction;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.Value;
import org.minbase.server.kv.WriteBatch;

import org.minbase.server.conf.Configuration;
import org.minbase.server.constant.Constants;
import org.minbase.server.kv.iterator.KeyValueIterator;
import org.minbase.server.kv.compaction.CompactionStrategy;
import org.minbase.server.kv.storage.storefilemanager.AbstractStoreFileManager;
import org.minbase.server.kv.storage.storefilemanager.level.LevelStoreFileManager;
import org.minbase.server.kv.storage.storefilemanager.tiered.TieredStoreFileManager;
import org.minbase.server.kv.utils.KeyUtil;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.ReentrantLock;
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
    private ThreadPoolExecutor flushThreadPool;

    /**
     * 文件压缩线程
     */
    private Compaction compaction;
    private CompactThread compactThread;
    /**
     * 上次刷写的序列号
     */
    private long lastFlushSequenceId;
    private ReentrantLock flushLock;

    public Store(String name, File dir, Configuration conf,
                 ThreadPoolExecutor flushThreadPool, Compaction compaction,
                 CompactThread compactThread) throws IOException {
        this.name = name;
        this.dir = dir;
        this.configuration = conf;
        this.flushThreadPool = flushThreadPool;

        this.memStore = new MemStore(conf);
        this.freezedMemStores = new ConcurrentLinkedDeque<MemStore>();

        this.rwLock = new ReentrantReadWriteLock();
        this.writeLock = rwLock.writeLock();
        this.readLock = rwLock.readLock();
        this.flushLock = new ReentrantLock();
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
        flushThreadPool.execute(new FlushTask(this));
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
            if (ByteUtil.byteEqual(keyValue.getKey().getInternalKey(), key.getInternalKey())) {
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

    public void setLastFlushSequenceId(long lastSyncSequenceId) {
        this.lastFlushSequenceId = lastSyncSequenceId;
    }

    public void clearOldLog(long lastSyncSequenceId) {

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
}
