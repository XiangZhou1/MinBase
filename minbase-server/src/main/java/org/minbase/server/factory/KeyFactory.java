package org.minbase.server.factory;

import org.minbase.server.constant.Constants;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyImpl;
import org.minbase.server.table.kv.InternalKey;

public class KeyFactory {
    // 0 KeyImpl
    // 1 InternalKey
    private int keyType = 0;

    public KeyFactory() {
    }

    public KeyFactory(int keyType) {
        this.keyType = keyType;
    }

    public Key latestKey(byte[] userKey) {
        switch (keyType) {
            case 0:
                return new KeyImpl(userKey, Constants.LATEST_VERSION);
            case 1:
                return new InternalKey(userKey, Constants.LATEST_VERSION);
            default:
                return null;
        }
    }

    public Key minKey(byte[] userKey) {
        switch (keyType) {
            case 0:
                return new KeyImpl(userKey, Long.MAX_VALUE);
            case 1:
                return new InternalKey(userKey, Long.MAX_VALUE);
            default:
                return null;
        }
    }

    public Key maxKey(byte[] userKey) {
        switch (keyType) {
            case 0:
                return new KeyImpl(userKey, Long.MIN_VALUE);
            case 1:
                return new InternalKey(userKey, Long.MIN_VALUE);
            default:
                return null;
        }
    }

    public Key newKey() {
        switch (keyType) {
            case 0:
                return new KeyImpl();
            case 1:
                return new InternalKey();
            default:
                return null;
        }
    }


}
