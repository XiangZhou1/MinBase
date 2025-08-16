package org.minbase.server.kv.compaction;

import org.minbase.server.conf.Configuration;
import org.minbase.server.constant.Constants;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.store.Scanner;
import org.minbase.server.kv.iterator.KeyValueIterator;
import org.minbase.server.kv.iterator.MergeIterator;
import org.minbase.server.kv.storage.StoreFile;
import org.minbase.server.kv.storage.StoreFileBuilder;
import org.minbase.server.kv.storage.StoreFileIterator;
import org.minbase.server.kv.storage.StoreFileManager;
import org.minbase.server.kv.utils.StoreFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class MinorCompactionPolicy implements CompactionPolicy {
    private static final Logger LOG = LoggerFactory.getLogger(MinorCompactionPolicy.class);
    Configuration configuration;
    private int minStoreFileCountToCompact = 3;
    private long storeFileLengthLimit = 512 * 1024 * 1024;

    public MinorCompactionPolicy(Configuration configuration) {
        this.minStoreFileCountToCompact = configuration.getInt(Constants.MIN_STORE_FILE_COUNT_TO_COMPACT_KEY,
                Constants.MIN_STORE_FILE_COUNT_TO_COMPACT_DEFAULT);
        this.storeFileLengthLimit = configuration.getLong(Constants.STORE_FILE_LENGTH_LIMIT_KEY,
                Constants.STORE_FILE_LENGTH_LIMIT_DEFAULT);
    }

    /**
     * compaction的目的
     * 1 减小小文件, 合并成一个大文件, 默认文件大小为512M
     * 2 减少key的重叠
     *
     * @param storeFileManager
     * @param filesToCompact
     * @return
     * @throws Exception
     */
    @Override
    public CompactionResult compact(StoreFileManager storeFileManager, List<StoreFile> filesToCompact, long minReadPoint) throws Exception {
        LOG.info("Compact, filesToCompactSize:{}", filesToCompact.size());
        Collections.sort(filesToCompact, new Comparator<StoreFile>() {
            @Override
            public int compare(StoreFile f1, StoreFile f2) {
                if (f1.getRawFile().length() != f2.getRawFile().length()) {
                    return (int) (f1.getRawFile().length() - f2.getRawFile().length());
                }
                if (f1.getRawFile().lastModified() < f2.getRawFile().lastModified()) {
                    return 1;
                } else {
                    return -1;
                }
            }
        });
        /**
         * 从最小文件开始合并
         * 1 如果最小文件大于512M, 且与别的文件有重叠， 则选择重叠的文件进行合并
         * 2 如果最小文件大于512M, 且与别的文件无重叠， 开始检查第二个文件
         * 3 如果最小文件小于512M, 且与别的文件无重叠， 则将最小文件与第二个文件进行合并， 一直到文件大小大于512M
         * 4 如果最小文件小于512M, 且与别的文件有重叠， 则则选择重叠的文件进行合并
         */
        List<StoreFile> selectedFiles = selectFilesToCompact(filesToCompact);
        if (selectedFiles == null || selectedFiles.isEmpty()) {
            return null;
        }
        List<KeyValueIterator> iterators = new ArrayList<>();
        for (StoreFile selectedFile : selectedFiles) {
            iterators.add(new StoreFileIterator(selectedFile.getStoreFileReader(), null, null, false));
        }
        MergeIterator mergeIterator = new MergeIterator(iterators);
        CompactionScanner scanner = new CompactionScanner(mergeIterator, minReadPoint);
        CompactionResult compactionResult = new CompactionResult();
        StoreFileBuilder storeFileBuilder = storeFileManager.newTmpStroeFile();
        while (scanner.hasNext()) {
            KeyValue next = scanner.next();
            if (next != null) {
                storeFileBuilder.add(next);
            }
            if (storeFileBuilder.length() >= storeFileLengthLimit) {
                compactionResult.addFileToAdd(storeFileBuilder.build());
                storeFileBuilder = storeFileManager.newTmpStroeFile();
            }
        }

        if (storeFileBuilder.length() > 0) {
            StoreFile storeFile = storeFileBuilder.build();
            storeFileManager.saveStoreFile(storeFile);
            compactionResult.addFileToAdd(storeFile);
        }
        compactionResult.addFilesToDelete(selectedFiles);
        return compactionResult;
    }

    private List<StoreFile> selectFilesToCompact(List<StoreFile> filesToCompact) {
        int storeFileIndex = 0;
        for (; storeFileIndex < filesToCompact.size(); storeFileIndex++) {
            StoreFile targetStoreFile = filesToCompact.get(storeFileIndex);
            long selectedLength = targetStoreFile.getRawFile().length();
            List<StoreFile> selectedFiles = new ArrayList<>();
            selectedFiles.add(targetStoreFile);
            for (int i = storeFileIndex + 1; i < filesToCompact.size(); i++) {
                StoreFile storeFile = filesToCompact.get(i);
                if (StoreFileUtil.isInternalKeyOverLay(targetStoreFile, storeFile)) {
                    selectedFiles.add(storeFile);
                    selectedLength += storeFile.getRawFile().length();
                }
            }
            // 有重叠， 则则选择重叠的文件进行合并
            if (selectedFiles.size() > 1) {
                return selectedFiles;
            } else {
                // 无重叠文件
                if (selectedLength >= storeFileLengthLimit) {
                    continue;
                } else {
                    for (int i = storeFileIndex + 1; i < filesToCompact.size(); i++) {
                        StoreFile storeFile = filesToCompact.get(i);
                        selectedFiles.add(storeFile);
                        selectedLength += storeFile.getRawFile().length();
                        if (selectedLength >= storeFileLengthLimit) {
                            return selectedFiles;
                        }
                    }
                    return selectedFiles;
                }
            }
        }
        return null;
    }

    @Override
    public boolean shouldCompact(StoreFileManager storeFileManager, List<StoreFile> filesToCompact) {
        return filesToCompact.size() > minStoreFileCountToCompact;
    }
}
