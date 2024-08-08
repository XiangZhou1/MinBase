package org.minbase.server.conf;

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
}
