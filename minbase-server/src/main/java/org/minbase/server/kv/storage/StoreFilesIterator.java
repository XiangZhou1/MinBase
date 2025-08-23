package org.minbase.server.kv.storage;

import org.minbase.common.utils.ByteUtil;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.iterator.KeyValueIterator;
import org.minbase.server.kv.iterator.MergeIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

public class StoreFilesIterator implements KeyValueIterator {
    private static final Logger LOG = LoggerFactory.getLogger(StoreFilesIterator.class);
    private StoreFileManager storeFileManager;
    private MergeIterator mergeIterator;
    private Set<StoreFile> storeFiles;
    private ReentrantLock lock = new ReentrantLock();
    private boolean closed = false;
    private Key startKey;

    public StoreFilesIterator(StoreFileManager storeFileManager, List<StoreFile> storeFiles,
                              List<KeyValueIterator> iterators) {
        this.storeFileManager = storeFileManager;
        this.mergeIterator = new MergeIterator(iterators);
        this.storeFiles = new HashSet<>(storeFiles);
    }

    public StoreFilesIterator(StoreFileManager storeFileManager, List<StoreFile> storeFiles,
                              List<KeyValueIterator> iterators, Key startKey, Key endKey) {
        this.storeFileManager = storeFileManager;
        this.mergeIterator = new MergeIterator(iterators);
        this.storeFiles = new HashSet<>(storeFiles);
        this.startKey = startKey;
        if (startKey != null) {
            this.mergeIterator.seek(startKey);
        }
    }

    @Override
    public KeyValue value() {
        return mergeIterator.value();
    }

    @Override
    public Key key() {
        return mergeIterator.key();
    }

    @Override
    public void seek(Key key) {
        lock.lock();
        try {
            mergeIterator.seek(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean hasNext() {
        lock.lock();
        try {
            return mergeIterator.hasNext();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public KeyValue next() {
        lock.lock();
        try {
            return mergeIterator.next();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            mergeIterator.close();
            storeFileManager.removeStoreFilesIterator(this);
        } finally {
            closed = true;
            lock.unlock();
        }
    }


    public boolean containStoreFile(List<StoreFile> storeFilesToDelete) {
        for (StoreFile storeFile : storeFilesToDelete) {
            if (storeFiles.contains(storeFile)) {
                System.out.println("Iter contain file need to delete:" + storeFile.getRawFile().getName());
                return true;
            }
        }
        return false;
    }

    public void updateStoreFiles(List<StoreFile> newStoreFiles) {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            Key currentKey = key();
            Key seekKey = currentKey == null ? startKey : currentKey;
            this.storeFiles = new HashSet<>(newStoreFiles);
            List<KeyValueIterator> iterators = new ArrayList<>();
            for (StoreFile storeFile : newStoreFiles) {
                iterators.add(new StoreFileIterator(storeFile.getStoreFileReader(), seekKey, mergeIterator.getEndKey(), true));
            }
            this.mergeIterator = new MergeIterator(iterators, seekKey, mergeIterator.getEndKey());
            if (currentKey != null) {
                if (hasNext()) {
                    next();
                }
            }

            boolean updateSuccess = false;
            if (currentKey == null && key() == null) {
                updateSuccess = true;
            }
            if (currentKey != null && key() != null) {
                if (ByteUtil.byteEqual(currentKey.encode(), key().encode())) {
                    updateSuccess = true;
                }
            }
            if (!updateSuccess) {
                LOG.error("updateStoreFileIterator, startKey:{}, currentKey before update:{}, currentKey after update:{}",
                        startKey, currentKey, key());
                throw new RuntimeException("updateStoreFileIterator, startKey:" + startKey
                        + ", currentKey before update:" + currentKey + ", currentKey after update:" + key());
            }
        } finally {
            lock.unlock();
        }
    }
}
