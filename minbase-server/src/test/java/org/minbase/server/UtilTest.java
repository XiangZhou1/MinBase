package org.minbase.server;


import org.junit.Test;
import org.minbase.common.utils.ByteUtil;

public class UtilTest {
    @Test
    public void test1() {
        int value = 124235;
        byte[] bytes = ByteUtil.intToByteArray(value);
        int i = ByteUtil.byteArrayToInt(bytes, 0);
        System.out.println(i);
    }

    @Test
    public void test2() {
        short value = 234;
        byte[] bytes = ByteUtil.shotToByteArray(value);
        short value2 = ByteUtil.byteArrayToShort(bytes, 0);
        System.out.println(value2);
    }

    @Test
    public void test3() {
        long value = 234232374982L;
        byte[] bytes = ByteUtil.longToByteArray(value);
        long value2 = ByteUtil.byteArrayToLong(bytes, 0);
        System.out.println(value2);
    }
}
