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
    public static final String MINBASE_CONF = "minbase.conf";
    public static final String KEY_COMPACTION_STRATEGY = "minbase.compaction_strategy";
    public static final String KEY_MAX_BLOCK_SIZE = "minbase.max_block_size";
    public static final String DATA_DIR_KEY = "minbase.data_dir";
    public static final String MEMSTORE_SIZE_LIMIT_KEY = "minbase.max_memtable_size";
    public static final String KEY_MAX_SSTABLE_SIZE = "minbase.max_sstable_size";
    public static final String KEY_WAL_SYNC_LEVEL = "minbase.wal_sync_level";
    public static final String KEY_WAL_FILE_LENGTH_LIMIT = "minbase.wal_file_length_limit";
    public static final String MAX_IM_MEM_STORE_NUM_KEY = "minbase.max_sstable_size";
    public static final String CACHE_SIZE_LIMIT_KEY = "minbase.cache_size_limit";
    public static final String STORE_FILE_SIZE_LIMIT_KEY = "minbase.store_file_size_limit";
    public static final String LEVEL_LIMIT_KEY = "minbase.level_limit";
    public static final String KEY_BLOCK_SIZE_LIMIT = "minbase.block_size_limit";
}
