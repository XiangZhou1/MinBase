package org.minbase.server.kv.storage;

import com.google.common.collect.Lists;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.conf.Configuration;
import org.minbase.server.constant.Constants;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.compaction.CompactionChecker;
import org.minbase.server.kv.compaction.CompactionPolicy;
import org.minbase.server.kv.compaction.MinorCompactionPolicy;
import org.minbase.server.kv.iterator.KeyValueIterator;
import org.minbase.server.kv.storage.block.DataBlock;
import org.minbase.server.kv.storage.cache.BlockCache;
import org.minbase.server.kv.storage.cache.LRUBlockCache;
import org.minbase.server.kv.store.Store;
import org.minbase.server.kv.utils.StoreFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class StoreFileManager {
    private static final Logger LOG = LoggerFactory.getLogger(StoreFileManager.class);
    protected CompactionPolicy compactionPolicy;
    private CompactionChecker compactionChecker;
    protected File storeDir;
    List<StoreFile> storeFiles = new LinkedList<>();
    private Configuration configuration;
    private BlockCache blockCache;
    protected ReentrantReadWriteLock updateFileLock = new ReentrantReadWriteLock();
    protected ReentrantReadWriteLock.ReadLock readLock = updateFileLock.readLock();
    protected ReentrantReadWriteLock.WriteLock writeLock = updateFileLock.writeLock();
    List<StoreFilesIterator> processingStoreFilesIterators = new ArrayList<>();
    private Store store;

    public StoreFileManager(File storeDir, Store store, Configuration configuration) throws IOException {
        this.configuration = configuration;
        this.storeDir = storeDir;
        this.store = store;
        if (!this.storeDir.exists()) {
            if (!this.storeDir.mkdirs()) {
                throw new IOException("Create store dir failed, store dir:" + this.storeDir.getAbsolutePath());
            }
        }
        this.blockCache = new LRUBlockCache(configuration.getLong(Constants.BLOCK_CACHE_LENGTH_LIMIT_KEY,
                Constants.BLOCK_CACHE_LENGTH_LIMIT_DEFAULT));
        this.compactionPolicy = new MinorCompactionPolicy(configuration);
        this.compactionChecker = new CompactionChecker(store.getStoreManager(), this);

        loadStoreFiles();

        this.compactionChecker.start();
    }

    public File getStoreDir() {
        return storeDir;
    }

    public CompactionPolicy getCompactionPolicy() {
        return compactionPolicy;
    }

    /// //////////////////////////////////////////////////////////////////////////
    // SSTable操作 load/add
    public void loadStoreFiles() throws IOException {
        List<Path> paths = Files.list(storeDir.getAbsoluteFile().toPath()).collect(Collectors.toList());
        for (Path path : paths) {
            if (path.getFileName().toString().endsWith("tmp")) {
                continue;
            }
            storeFiles.add(loadStoreFile(new File(path.toUri())));
        }
    }

    protected RandomAccessFile getStoreFile(String storeId, String mode) throws FileNotFoundException {
        String filePath = getFilePath(storeId);
        return new RandomAccessFile(filePath, mode);
    }

    protected String getFilePath(String storeId) {
        return getStoreDir().getPath() + File.separator + storeId;
    }

    protected StoreFile loadStoreFile(File rawFile) throws IOException {
        StoreFile storeFile = new StoreFile(rawFile.getName());
        storeFile.setRawFile(rawFile);
        // 加载非数据块部分的其余块信息
        storeFile.getStoreFileReader().loadNonDataBlocks();
        return storeFile;
    }

    public void addStoreFile(StoreFile storeFile) {
        writeLock.lock();
        try {
            storeFiles.add(storeFile);
        } finally {
            writeUnLock();
        }
    }

    public void updateStoreFiles(List<StoreFile> toAdd, List<StoreFile> toDelete) {
        writeLock.lock();
        try {
            this.storeFiles.addAll(toAdd);
            this.storeFiles.removeAll(toDelete);
            if (storeFiles.size() == 0) {
                System.out.println("empty");
            }
        } finally {
            writeUnLock();
        }
    }

    public void saveStoreFile(StoreFile storeFile) throws IOException {
        File file = new File(storeDir, storeFile.getStoreId() + ".tmp");
        storeFile.setRawFile(file);
        try (FileOutputStream outputStream = new FileOutputStream(storeFile.getRawFile())) {
            storeFile.encodeToStream(outputStream);
            outputStream.flush();
            outputStream.getChannel().force(true);
        }
        File file2 = new File(storeDir, storeFile.getStoreId());
        file.renameTo(file2);
        storeFile.setRawFile(file2);
    }

    public StoreFileBuilder newTmpStroeFile() throws IOException {
        long sotreFileBlockLengthLimit = configuration.getLong(Constants.STORE_FILE_BLOCK_LENGTH_LIMIT_KEY, Constants.STORE_FILE_BLOCK_LENGTH_LIMIT_DEFAULT);
        StoreFileBuilder storeFileBuilder = new StoreFileBuilder(sotreFileBlockLengthLimit);
        return storeFileBuilder;
    }

    public StoreFile completeStoreFile(StoreFileBuilder storeFileBuilder) throws IOException {
        StoreFile storeFile = storeFileBuilder.build();
        saveStoreFile(storeFile);
        addStoreFile(storeFile);
        cacheDataBlocks(storeFile);
        return storeFile;
    }

    public void cacheDataBlocks(StoreFile storeFile) {
        List<DataBlock> dataBlocks = storeFile.getDataBlocks();
        for (int i = 0; i < dataBlocks.size(); i++) {
            DataBlock dataBlock = dataBlocks.get(i);
            blockCache.put(storeFile, i, dataBlock);
        }
        dataBlocks.clear();
    }

    public StoreFilesIterator newStoreFilesIterator(Key startKey, Key endKey, boolean cache) {
        List<KeyValueIterator> iterators = new ArrayList<>();
        List<StoreFile> storeFileList = new ArrayList<>();

        boolean isGetSpecificKey = startKey != null && endKey != null &&
                ByteUtil.ByteEqual(startKey.getInternalKey(), endKey.getInternalKey());
        readLock();
        try {
            for (StoreFile storeFile : storeFiles) {
                if (isGetSpecificKey && !storeFile.getBloomFilter().contains(startKey.getInternalKey())
                        && !StoreFileUtil.isOverlay(storeFile, startKey, endKey)) {
                    continue;
                }
                if (!StoreFileUtil.isOverlay(storeFile, startKey, endKey)) {
                    continue;
                }
                storeFileList.add(storeFile);
                iterators.add(new StoreFileIterator(storeFile.getStoreFileReader(), startKey, endKey, cache));
            }
            return new StoreFilesIterator(this, storeFileList, iterators, startKey, endKey);
        } finally {
            readUnLock();
        }
    }

    public void readLock() {
        readLock.lock();
    }

    public void readUnLock() {
        readLock.unlock();
    }

    public void writeLock() {
        writeLock.lock();
    }

    public void writeUnLock() {
        writeLock.unlock();
    }

    public void removeStoreFilesIterator(StoreFilesIterator storeFilesIterator) {
        writeLock();
        try {
            processingStoreFilesIterators.remove(storeFilesIterator);
        } finally {
            writeUnLock();
        }
    }

    public void updateStoreFilesIterators(List<StoreFile> storeFilesToDelete) {
        readLock();
        try {
            for (StoreFilesIterator processingStoreFilesIterator : processingStoreFilesIterators) {
                if (processingStoreFilesIterator.containStoreFile(storeFilesToDelete)) {
                    processingStoreFilesIterator.updateStoreFiles(Lists.newArrayList(storeFiles));
                }
            }
        } finally {
            readUnLock();
        }
    }

    public List<StoreFile> getStoreFilesToCompact() {
        List<StoreFile> storeFilesToCompact = null;
        readLock();
        try {
            storeFilesToCompact = new ArrayList<>(storeFiles);
        } finally {
            readUnLock();
        }
        return storeFilesToCompact;
    }

    public void deleteStoreFiles(List<StoreFile> filesToDelete) {
        for (StoreFile storeFile : filesToDelete) {
            if (storeFile.getRawFile().exists()) {
                if (!storeFile.getRawFile().delete()) {
                    LOG.warn("Delete store file failed, store file:" + storeFile.getRawFile().getAbsolutePath());
                }
            }
        }
    }

    public void requestCompaction() {
        this.compactionChecker.requestCompaction();
    }
}
