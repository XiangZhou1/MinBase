package org.minbase.server.constant;

public class Constants {

    public static final int INTEGER_LENGTH = Integer.BYTES;
    public static final int SHORT_LENGTH = Short.BYTES;
    public static final int LONG_LENGTH = Long.BYTES;
    public static final int BYTE_LENGTH = Byte.BYTES;

    // config 配置
    public static final String MINBASE_CONF = "minbase.conf";
    public static final String KEY_COMPACTION_STRATEGY = "minbase.compaction_strategy";
    public static final String KEY_MAX_CACHE_SIZE = "minbase.max_cache_size";
    public static final String KEY_MAX_BLOCK_SIZE = "minbase.max_block_size";
    public static final String KEY_DATA_DIR = "minbase.data_dir";
    public static final String KEY_MAX_MEMTABLE_SIZE = "minbase.max_memtable_size";
    public static final String KEY_MAX_SSTABLE_SIZE = "minbase.max_sstable_size";
    public static final String KEY_WAL_SYNC_LEVEL = "minbase.wal_sync_level";
    public static final String KEY_WAL_FILE_LENGTH_LIMIT = "minbase.wal_file_length_limit";

    public static final String MEM_STORE_LENGTH_LIMIT_KEY = "memstore.length.limit";
    public static final long MEM_STORE_LENGTH_LIMIT_DEFAULT = 128 * 1024 * 1024;
    public static final int FLUSH_THREAD_MAX_POOL_SIZE_DEFAULT = 4;
    public static final String FLUSH_THREAD_MAX_POOL_SIZE_KEY = "flush.thread.max.pool.size";
}
