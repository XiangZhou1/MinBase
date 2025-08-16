package org.minbase.server.kv.storage;

import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.iterator.KeyValueIterator;
import org.minbase.server.kv.iterator.MergeIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class StoreFilesIterator implements KeyValueIterator {
    private StoreFileManager storeFileManager;
    private MergeIterator mergeIterator;
    private List<StoreFile> storeFiles;
    private ReentrantLock lock = new ReentrantLock();

    public StoreFilesIterator(StoreFileManager storeFileManager, List<StoreFile> storeFiles,
                              List<KeyValueIterator> iterators) {
        this.storeFileManager = storeFileManager;
        this.mergeIterator = new MergeIterator(iterators);
        this.storeFiles = storeFiles;
    }

    public StoreFilesIterator(StoreFileManager storeFileManager, List<StoreFile> storeFiles,
                              List<KeyValueIterator> iterators, Key startKey, Key endKey) {
        this.storeFileManager = storeFileManager;
        this.mergeIterator = new MergeIterator(iterators);
        this.storeFiles = storeFiles;
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
            lock.unlock();
        }
    }


    public boolean containStoreFile(List<StoreFile> storeFilesToDelete) {
        for (StoreFile storeFile : storeFilesToDelete) {
            if (storeFiles.contains(storeFile)) {
                return true;
            }
        }
        return false;
    }

    public void updateStoreFiles(ArrayList<StoreFile> storeFiles) {
        lock.lock();
        try {
            Key currentKey = key();
            System.out.println(Thread.currentThread() + ": key before updateStoreFiles:" + currentKey);
            this.storeFiles = storeFiles;
            List<KeyValueIterator> iterators = new ArrayList<>();
            for (StoreFile storeFile : storeFiles) {
                iterators.add(new StoreFileIterator(storeFile.getStoreFileReader(), currentKey, mergeIterator.getEndKey(), true));
            }
            this.mergeIterator = new MergeIterator(iterators, currentKey, mergeIterator.getEndKey());
            if (currentKey != null) {
                if (hasNext()) {
                    next();
                }
            }
            System.out.println(Thread.currentThread() + ": key after updateStoreFiles:" + currentKey);
        } finally {
            lock.unlock();
        }
    }
}
