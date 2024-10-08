package org.minbase.server.storage.block;


import org.junit.Test;
import org.minbase.server.kv.KeyImpl;

public class MetaBlockTest {
    @Test
    public void test1() {
        // int offset, Key firstKey, Key lastKey, int keyValueNum
        MetaBlock metaBlock = new MetaBlock(1000, KeyImpl.latestKey("k1".getBytes()), KeyImpl.latestKey("k2".getBytes()), 7);

        MetaBlock metaBlock1 = new MetaBlock();
        metaBlock1.decode(metaBlock.encode(), 0);

        System.out.println(new String(metaBlock.encode()));
        System.out.println(new String(metaBlock1.encode()));
        assert metaBlock.toString().equals(metaBlock1.toString());
    }
}
