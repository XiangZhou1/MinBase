package org.minbase.server.utils;

import org.minbase.server.constant.Constants;
import org.minbase.server.factory.KeyFactory;
import org.minbase.server.kv.Key;
import org.minbase.server.table.kv.InternalKey;

public class KeyUtils {
    public static KeyFactory keyFactory = new KeyFactory(1);


    public static Key latestKey(byte[] userKey) {
        return keyFactory.latestKey(userKey);
    }
    public static Key latestKey(byte[] userKey, byte[] column) {
        return new InternalKey(userKey, column, Constants.LATEST_VERSION);
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
