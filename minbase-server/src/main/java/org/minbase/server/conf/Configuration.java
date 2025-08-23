package org.minbase.server.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.minbase.server.constant.Constants.MINBASE_CONF;


public class Configuration {
    private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);
    private Properties config = new Properties();

    public Configuration() {
    }

    public Configuration(File configFile) throws IOException {
        try (InputStream resourceAsStream = new FileInputStream(configFile)) {
            //通过Properties加载配置文件
            config.load(resourceAsStream);
        } catch (IOException e) {
            LOG.error("Load config file " + configFile.getName() + " error", e);
            throw e;
        }
    }

    public long getLong(String key, long defaultValue) {
        String property = config.getProperty(key);
        if (property == null) {
            return defaultValue;
        } else {
            return Long.parseLong(property);
        }
    }

    public int getInt(String key, int defaultValue) {
        String property = config.getProperty(key);
        if (property == null) {
            return defaultValue;
        } else {
            return Integer.parseInt(property);
        }
    }

    public String get(String key, String defaultValue) {
        return config.getProperty(key, defaultValue);
    }
}
