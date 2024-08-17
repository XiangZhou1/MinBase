package org.minbase.server.conf;

import org.minbase.common.utils.Util;
import org.minbase.server.constant.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

import static org.minbase.server.constant.Constants.MINBASE_CONF;


public class Config {

    private static final Logger logger = LoggerFactory.getLogger(Config.class);
    private static Properties config = new Properties();
    static {
        try (InputStream resourceAsStream =
                     Config.class.getClassLoader().getResourceAsStream(MINBASE_CONF)) {
            //通过Properties加载配置文件
            config.load(resourceAsStream);
        } catch (Exception e) {
            logger.error("Load config file(minbase.conf) error", e);
            System.exit(-1);
        }
    }
    public static String get(String key) {
        return config.getProperty(key);
    }

    /*
        dataDir/
            /table1
                /tableMeta
                /manifest
                /level0
                /level1
                /......
            /table2
     */
    public static String DATA_DIR = get(Constants.DATA_DIR_KEY);

    // 一个memstore的内存大小限制
    public static long MEM_STORE_SIZE_LIMIT = Util.parseWithSizeUnit(Config.get(Constants.MEMSTORE_SIZE_LIMIT_KEY));
    // immutable memsotre 的数量限制
    public static  int MAX_IM_MEM_STORE_NUM = Integer.parseInt(Config.get(Constants.MAX_IM_MEM_STORE_NUM_KEY));
    // 缓存大小
    public static long CACHE_SIZE_LIMIT = Util.parseWithSizeUnit(Config.get(Constants.CACHE_SIZE_LIMIT_KEY));

    public static long STORE_FILE_SIZE_LIMIT = Util.parseWithSizeUnit(Config.get(Constants.STORE_FILE_SIZE_LIMIT_KEY));
    public static long LEVEL_LIMIT = Util.parseWithSizeUnit(Config.get(Constants.LEVEL_LIMIT_KEY));
    public static long BLOCK_SIZE_LIMIT =  Util.parseWithSizeUnit(Config.get(Constants.KEY_BLOCK_SIZE_LIMIT));
    public static final long WAL_FILE_LENGTH_LIMIT = Util.parseWithSizeUnit(Config.get(Constants.KEY_WAL_FILE_LENGTH_LIMIT));
}
