package org.minbase.server.conf;

import java.io.InputStream;
import java.util.Properties;

public class Config {
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
            e.printStackTrace();
            //throw new RuntimeException(e);
        }
    }

    public static String get(String key) {
        return config.getProperty(key);
    }
}
