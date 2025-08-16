package org.minbase.server.kv.storage.block;

import org.junit.Test;
import org.minbase.server.kv.storage.block.BloomFilterBlock;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class BloomFilterTest {
    @Test
    public void testFilter() {
        BloomFilterBlock bloomFilter = new BloomFilterBlock();
        byte[] encode = bloomFilter.encode();
        System.out.println("length:" + encode.length);
        bloomFilter.add("k1".getBytes(StandardCharsets.UTF_8));
        System.out.println(bloomFilter.contains("k1".getBytes(StandardCharsets.UTF_8)));
        System.out.println(bloomFilter.contains("k2".getBytes(StandardCharsets.UTF_8)));

        byte[] encode2 = bloomFilter.encode();
        System.out.println("length:" + encode2.length);
    }

    @Test
    public void testFilter2() {
        BloomFilterBlock bloomFilter = new BloomFilterBlock();
        for (int i = 0; i < 50000; i++) {
            bloomFilter.add(("k" + i).getBytes(StandardCharsets.UTF_8));
        }
        int errCount = 0;
        for (int i = 50000; i < 100000; i++) {
            if (bloomFilter.contains(("k" + i).getBytes(StandardCharsets.UTF_8))) {
                errCount++;
            }
        }
        System.out.println(errCount);
        // errCount = 332
        // 0.368%
    }


}
