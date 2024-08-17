package org.minbase.server.utils;

import org.minbase.server.factory.KeyFactory;
import org.minbase.server.kv.Key;

public class KeyUtils {
    private static KeyFactory keyFactory = new KeyFactory(1);


    public static Key latestKey(byte[] userKey) {
        return keyFactory.latestKey(userKey);
    }

    public static Key minKey(byte[] userKey) {
        return keyFactory.minKey(userKey);
    }

    public static Key maxKey(byte[] userKey) {
        return keyFactory.maxKey(userKey);
    }

    public static Key newKey() {
        return keyFactory.newKey();
    }
}
