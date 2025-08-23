package org.minbase.server.constant;

import org.minbase.server.table.wal.SyncLevel;

public class Constants {

    public static final int INTEGER_LENGTH = Integer.BYTES;
    public static final int SHORT_LENGTH = Short.BYTES;
    public static final int LONG_LENGTH = Long.BYTES;
    public static final int BYTE_LENGTH = Byte.BYTES;

    // config 配置
    public static final String MINBASE_CONF = "minbase.conf";

    public static final String MEM_STORE_LENGTH_LIMIT_KEY = "memstore.length.limit";
    public static final long MEM_STORE_LENGTH_LIMIT_DEFAULT = 128 * 1024 * 1024;
    public static final String STORE_FILE_LENGTH_LIMIT_KEY = "stor.file.length.limit";
    public static final long STORE_FILE_LENGTH_LIMIT_DEFAULT = 512 * 1024 * 1024;
    public static final String MIN_STORE_FILE_COUNT_TO_COMPACT_KEY = "min.store.file.count.to.compact";
    public static final int MIN_STORE_FILE_COUNT_TO_COMPACT_DEFAULT = 3;
    public static final String STORE_FILE_BLOCK_LENGTH_LIMIT_KEY = "stor.file.block.length.limit";
    public static final long STORE_FILE_BLOCK_LENGTH_LIMIT_DEFAULT = 1 * 1024;
    public static final String BLOCK_CACHE_LENGTH_LIMIT_KEY = "block.cache.length.limit";
    public static final long BLOCK_CACHE_LENGTH_LIMIT_DEFAULT = 128 * 1024 * 1024;
    public static final String STORE_DIR_KEY = "stor.dir";
    public static final String STORE_DIR_DEFAULT = "data1";
    public static final String WAL_LOG_COUNT_LIMIT_KEY = "wal.log.count.limit";
    public static final int WAL_LOG_COUNT_LIMIT_DEFAULT = 10000;
    public static final String WAL_FILE_LENGTH_LIMIT = "wal.file.length.limit" ;
    public static final long WAL_FILE_LENGTH_LIMIT_DEFALUT = 10 * 1024 *1024;
    public static final String WAL_SYNC_LEVEL_KEY = "wal.sync.level";
    public static final String WAL_SYNC_LEVEL_DEFAULT = SyncLevel.SYNC.toString();
    public static final String WAL_FORE_FLUSH_FILE_NUM_KEY = "wal.force.flush.file.num";
    public static final int WAL_FORE_FLUSH_FILE_NUM_DEFAULT = 50;
    public static final String WAL_FORCE_FLUSH_TIME_KEY = "wal.fore.flush.time";
    public static final long WAL_FORCE_FLUSH_TIME_DEFAULT = 60 * 60 * 1000;
    public static final String CLEAR_OLD_LOG_CHECK_INTERVAL_KEY = "clear.old.log.check.interval";
    public static final long CLEAR_OLD_LOG_CHECK_INTERVAL_DEFAULT = 5 * 60 * 1000;
    public static final String COMPACT_CHECK_INTERVAL_KEY = "compact.check.interval";
    public static final long COMPACT_CHECK_INTERVAL_DEFAULT = 5 * 60 * 1000;
}
