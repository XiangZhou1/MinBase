package org.minbase.server.storage.block;


import org.junit.Test;
import org.minbase.server.kv.KeyImpl;
import org.minbase.server.utils.KeyUtils;

public class MetaBlockTest {

    public static final byte[] key = "k1".getBytes();
    public static final byte[] column = "c1".getBytes();
    public static final byte[] key2 = "k2".getBytes();
    public static final byte[] column2 = "c2".getBytes();
    @Test
    public void test1() {
        // int offset, Key firstKey, Key lastKey, int keyValueNum
        MetaBlock metaBlock = new MetaBlock(1000, KeyUtils.latestKey(key, column), KeyUtils.latestKey(key2, column2), 7);

        MetaBlock metaBlock1 = new MetaBlock();
        metaBlock1.decode(metaBlock.encode(), 0);

        System.out.println(new String(metaBlock.encode()));
        System.out.println(new String(metaBlock1.encode()));
        assert metaBlock.toString().equals(metaBlock1.toString());
    }
}
