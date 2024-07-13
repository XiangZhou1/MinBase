package org.minbase.server;


import org.junit.Test;
import org.minbase.common.utils.ByteUtils;

public class UtilTest {
    @Test
    public void test1() {
        int value = 124235;
        byte[] bytes = ByteUtils.intToByteArray(value);
        int i = ByteUtils.byteArrayToInt(bytes, 0);
        System.out.println(i);
    }

    @Test
    public void test2() {
        short value = 234;
        byte[] bytes = ByteUtils.shotToByteArray(value);
        short value2 = ByteUtils.byteArrayToShort(bytes, 0);
        System.out.println(value2);
    }

    @Test
    public void test3() {
        long value = 234232374982L;
        byte[] bytes = ByteUtils.longToByteArray(value);
        long value2 = ByteUtils.byteArrayToLong(bytes, 0);
        System.out.println(value2);
    }
}
