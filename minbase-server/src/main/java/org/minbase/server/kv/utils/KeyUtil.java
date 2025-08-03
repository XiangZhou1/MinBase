package org.minbase.server.kv.utils;

import org.minbase.server.kv.Key;

public class KeyUtil {
    /**
     * 最新版本的key
     *
     * @param userKey 用户的key
     * @return
     */
    public static Key latestKey(byte[] userKey) {
        return new Key(userKey, Key.LATEST_VERSION);
    }

    public static Key minVersionKey(byte[] userKey) {
        return new Key(userKey, Long.MAX_VALUE);
    }

    public static Key maxVersionKey(byte[] userKey) {
        return new Key(userKey, Long.MIN_VALUE);
    }

    public static Key latestVersionKey(byte[] internalKey) {
        return new Key(internalKey, Key.LATEST_VERSION);
    }

    public static Key earliestVersionKey(byte[] internalKey) {
        return new Key(internalKey, Key.EARLIEST_VERSION);
    }
}
