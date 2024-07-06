package org.minbase.server.constant;

public class Constants {

    public static final int INTEGER_LENGTH = 4;
    public static final int SHORT_LENGTH = 2;
    public static final int LONG_LENGTH = 8;

    // 表示查找最新版本
    public static final long LATEST_VERSION = Long.MAX_VALUE;
    // 表示查找最新版本
    public static final long NO_VERSION = -1;

    // config 配置
    public static final String KEY_COMPACTION_STRATEGY = "minbase.compaction_strategy";
    public static final String KEY_MAX_CACHE_SIZE = "minbase.max_cache_size";
    public static final String KEY_MAX_BLOCK_SIZE = "minbase.max_block_size";
    public static final String KEY_DATA_DIR = "minbase.data_dir";
    public static final String KEY_MAX_MEMTABLE_SIZE = "minbase.max_memtable_size";
    public static final String KEY_MAX_SSTABLE_SIZE = "minbase.max_sstable_size";
    public static final String KEY_WAL_SYNC_LEVEL = "minbase.wal_sync_level";
}
