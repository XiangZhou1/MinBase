package org.minbase.server.utils;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;

public class ByteUtils {
    public static Comparator<byte[]> BYTE_ORDER_COMPARATOR = new Comparator<byte[]>() {
        @Override
        public int compare(byte[] o1, byte[] o2) {
            int minLen = Math.min(o1.length, o2.length);
            for (int i = 0; i < minLen; i++) {
                if (o1[i] != o2[i]) {
                    return o1[i] - o2[i];
                }
            }
            return o1.length - o2.length;
        }
    };

    public static byte[] toBytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    public static boolean inRange(byte[] key, byte[] firstKey, byte[] lastKey) {
        return BYTE_ORDER_COMPARATOR.compare(key, firstKey) >= 0 && BYTE_ORDER_COMPARATOR.compare(key, lastKey) <= 0;
    }

    public static boolean byteEqual(byte[] key, byte[] targetKey) {
        return BYTE_ORDER_COMPARATOR.compare(key, targetKey) == 0;
    }

    public static boolean byteGreaterOrEqual(byte[] key, byte[] targetKey) {
        return BYTE_ORDER_COMPARATOR.compare(key, targetKey) >= 0;
    }

    public static boolean byteLess(byte[] key, byte[] targetKey) {
        return BYTE_ORDER_COMPARATOR.compare(key, targetKey) < 0;
    }

    public static byte[] shotToByteArray(short value) {
        byte[] result = new byte[2];
        result[0] = (byte) ((value >> 8) & 0xFF);
        result[1] = (byte) (value & 0xFF);
        return result;
    }

    public static short byteArrayToShort(byte[] byteArray, int pos) {
        short value = 0;
        for (int i = 0; i < 2; i++) {
            int shift = (1 - i) * 8;
            value += (byteArray[i + pos] & 0xFF) << shift;
        }
        return value;
    }

    /**
     * int到byte[] 由高位到低位
     * @param i 需要转换为byte数组的整行值。
     * @return byte数组
     */
    public static byte[] intToByteArray(int i) {
        byte[] result = new byte[4];
        result[0] = (byte) ((i >> 24) & 0xFF);
        result[1] = (byte) ((i >> 16) & 0xFF);
        result[2] = (byte) ((i >> 8) & 0xFF);
        result[3] = (byte) (i & 0xFF);
        return result;
    }

    /**
     * byte[]转int
     * @param bytes 需要转换成int的数组
     * @return int值
     */
    public static int byteArrayToInt(byte[] bytes, int pos) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            int shift = (3 - i) * 8;
            value += (bytes[i + pos] & 0xFF) << shift;
        }
        return value;
    }


    /**
     * int到byte[] 由高位到低位
     * @param i 需要转换为byte数组的整行值。
     * @return byte数组
     */
    public static byte[] longToByteArray(long i) {
        byte[] result = new byte[8];
        result[0] = (byte) ((i >> 56) & 0xFF);
        result[1] = (byte) ((i >> 48) & 0xFF);
        result[2] = (byte) ((i >> 40) & 0xFF);
        result[3] = (byte) ((i >> 32) & 0xFF);
        result[4] = (byte) ((i >> 24) & 0xFF);
        result[5] = (byte) ((i >> 16) & 0xFF);
        result[6] = (byte) ((i >> 8) & 0xFF);
        result[7] = (byte) (i & 0xFF);
        return result;
    }

    /**
     * byte[]转int
     * @param bytes 需要转换成int的数组
     * @return int值
     */
    public static long byteArrayToLong(byte[] bytes, int pos) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            int shift = (7 - i) * 8;
            value += ((long) (bytes[i + pos] & 0xFF)) << shift;
        }
        return value;
    }
}
