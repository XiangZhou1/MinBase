package org.minbase.server.kv.utils;

import org.minbase.common.utils.ByteUtil;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.storage.StoreFile;

public class StoreFileUtil {
    /**
     * 判断两个文件的internalKey是否有重叠
     *
     * @param storeFile1 第一个文件
     * @param storeFile2 第二个文件
     * @return true 有重叠 false 无重叠
     */
    public static boolean isInternalKeyOverLay(StoreFile storeFile1, StoreFile storeFile2) {
        Key startKey1 = storeFile1.getFirstKey();
        Key endKey1 = storeFile1.getLastKey();
        Key startKey2 = storeFile2.getFirstKey();
        Key endKey2 = storeFile2.getLastKey();

        if (ByteUtil.BYTE_ORDER_COMPARATOR.compare(startKey1.getInternalKey(), startKey2.getInternalKey()) <= 0) {
            return ByteUtil.BYTE_ORDER_COMPARATOR.compare(endKey1.getInternalKey(), startKey2.getInternalKey()) >= 0;
        } else {
            return ByteUtil.BYTE_ORDER_COMPARATOR.compare(endKey2.getInternalKey(), startKey1.getInternalKey()) >= 0;
        }
    }

    public static boolean isOverlay(StoreFile storeFile, Key startKey, Key endKey) {
        if (startKey == null && endKey == null) {
            return true;
        }
        Key firstKey = storeFile.getFirstKey();
        Key lastKey = storeFile.getLastKey();
        if (startKey == null) {
            if (firstKey.compareTo(endKey) >= 0) {
                return false;
            } else {
                return true;
            }
        }

        if (endKey == null) {
            if (lastKey.compareTo(startKey) < 0) {
                return false;
            } else {
                return true;
            }
        }

        if (startKey.compareTo(lastKey) > 0) {
            return false;
        }
        if (firstKey.compareTo(endKey) >= 0) {
            return false;
        }
        return true;
    }
}
