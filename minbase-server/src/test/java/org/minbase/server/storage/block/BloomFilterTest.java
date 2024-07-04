package org.minbase.server.storage.block;

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class BloomFilterTest {
    @Test
    public void testFilter(){
        BloomFilterBlock bloomFilter = new BloomFilterBlock();
        byte[] encode = bloomFilter.encode();
        System.out.println(Arrays.toString(encode));
        bloomFilter.add("k1".getBytes(StandardCharsets.UTF_8));
        System.out.println( bloomFilter.mightContain("k1".getBytes(StandardCharsets.UTF_8)));
        System.out.println( bloomFilter.mightContain("k2".getBytes(StandardCharsets.UTF_8)));

        byte[] encode2 = bloomFilter.encode();
        System.out.println(encode2.length);
    }
}
