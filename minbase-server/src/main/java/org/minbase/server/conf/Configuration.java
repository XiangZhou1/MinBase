package org.minbase.server.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

import static org.minbase.server.constant.Constants.MINBASE_CONF;


public class Configuration {
    private static final Logger logger = LoggerFactory.getLogger(Configuration.class);
    private static Properties config = new Properties();

    public static String get(String key) {
        return config.getProperty(key);
    }

    static {
        try (InputStream resourceAsStream =
                     Configuration.class.getClassLoader().getResourceAsStream(MINBASE_CONF)) {
            //通过Properties加载配置文件
            config.load(resourceAsStream);
        } catch (Exception e) {
            logger.error("Load config file(minbase.conf) error", e);
            System.exit(-1);
        }
    }

    public long getLong(String key, long defaultValue) {
        return defaultValue;
    }
}
