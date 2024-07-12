package org.minbase.server.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;


public class Config {
    private static final Logger logger = LoggerFactory.getLogger(Config.class);
    private static Properties config = new Properties();

    static {
        InputStream resourceAsStream =
                Config.class.getClassLoader().getResourceAsStream("minbase.conf");
        try {
            //通过Properties加载配置文件
            config.load(resourceAsStream);
            //关闭输入流
            resourceAsStream.close();
        } catch (Exception e) {
            logger.error("Load config file(minbase.conf) error", e);
            System.exit(-1);
        }
    }

    public static String get(String key) {
        return config.getProperty(key);
    }
}
