package org.minbase.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {
    private static final Logger logger = LoggerFactory.getLogger(Util.class);

    public static long MB = 1024 * 1024;

    public static long parseUnit(String val) {
        if (val.endsWith("MB") || val.endsWith("mb")) {
            return Long.parseLong(val.substring(0, val.length() - 2)) * MB;
        } else {
            return Long.parseLong(val);
        }
    }

    public static String fillZero(int i) {
        String value = String.valueOf(i);
        if (value.length() < 8) {
            return "00000000".substring(value.length()) + value;
        }
        return value;
    }


    public static void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        }
    }
}
