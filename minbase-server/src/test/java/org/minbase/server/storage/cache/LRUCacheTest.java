package org.minbase.server.storage.cache;


import org.junit.Test;
import org.minbase.server.kv.storage.block.DataBlock;
import org.minbase.server.kv.storage.cache.LRUBlockCache;

public class LRUCacheTest {
    @Test
    public void test1() {
        LRUBlockCache blockCache = (LRUBlockCache) LRUBlockCache.BlockCache;
        for (int i = 0; i < 10; i++) {
            String blockId = String.valueOf(i);
            DataBlock block = new DataBlock();
            block.setBlockId(blockId);
            blockCache.put(blockId, block);
        }

        System.out.println(blockCache.list());

        blockCache.get("1");
        System.out.println(blockCache.list());
        blockCache.get("2");
        System.out.println(blockCache.list());
        blockCache.evict();
        System.out.println(blockCache.list());
        blockCache.evict();
        System.out.println(blockCache.list());
        blockCache.evict();
        System.out.println(blockCache.list());
        blockCache.evict();
        System.out.println(blockCache.list());blockCache.evict();
        System.out.println(blockCache.list());
        blockCache.evict();
        System.out.println(blockCache.list());
        blockCache.evict();
        System.out.println(blockCache.list());
        blockCache.evict();
        System.out.println(blockCache.list());
        blockCache.evict();
        System.out.println(blockCache.list());
        blockCache.evict();
        System.out.println(blockCache.list());
        blockCache.evict();
        System.out.println(blockCache.list());
        blockCache.evict();
        System.out.println(blockCache.list());


        for (int i = 0; i < 10; i++) {
            String blockId = String.valueOf(i);
            DataBlock block = new DataBlock();
            block.setBlockId(blockId);
            blockCache.put(blockId, block);
        }
        System.out.println(blockCache.list());

    }

    @Test
    public void test2() {
        LRUBlockCache blockCache = (LRUBlockCache) LRUBlockCache.BlockCache;
        for (int i = 0; i < 10; i++) {
            String blockId = String.valueOf(i);
            DataBlock block = new DataBlock();
            block.setBlockId(blockId);
            blockCache.put(blockId, block);
        }

        System.out.println(blockCache.list());

        blockCache.evict("1");
        System.out.println(blockCache.list());
    }
}
