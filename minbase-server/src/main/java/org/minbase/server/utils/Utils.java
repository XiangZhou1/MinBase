package org.minbase.server.utils;

public class Utils {
    public static long MB = 1024 * 1024;

    public static long parseUnit(String val) {
        if (val.endsWith("MB") || val.endsWith("mb")) {
            return Long.parseLong(val.substring(0, val.length() - 2)) * MB;
        } else {
            return Long.parseLong(val);
        }
    }
}
