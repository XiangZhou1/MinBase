package org.minbase.server.storage.store;


import org.minbase.common.utils.Util;
import org.minbase.server.conf.Config;
import org.minbase.server.constant.Constants;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.storage.block.BloomFilterBlock;
import org.minbase.server.storage.block.DataBlock;
import org.minbase.server.storage.block.DataBlockBuilder;
import org.minbase.server.storage.block.MetaBlock;

import java.util.UUID;

public class StoreFileBuilder {
    public int MAX_BLOCK_SIZE = (int) Util.parseUnit(Config.get(Constants.KEY_MAX_BLOCK_SIZE));
    StoreFile storeFile;

    Key firstKey;
    Key lastKey;
    int blockOffset;
    int length = 0;
    BloomFilterBlock bloomFilter;
    DataBlockBuilder blockBuilder;


    public StoreFileBuilder() {
        blockBuilder = new DataBlockBuilder();
        bloomFilter = new BloomFilterBlock();
        storeFile = new StoreFile(UUID.randomUUID().toString(), bloomFilter);
    }

    public int length() {
        return length;
    }

    public void add(KeyValue kv){
        length += kv.length();
        bloomFilter.add(kv.getKey().getUserKey());

        if (firstKey == null || firstKey.compareTo(kv.getKey()) > 0) {
            firstKey = kv.getKey();
        }
        if (lastKey == null || lastKey.compareTo(kv.getKey())  < 0) {
            lastKey = kv.getKey();
        }

        blockBuilder.add(kv);

        if (blockBuilder.length() > MAX_BLOCK_SIZE) {
            DataBlock block = blockBuilder.build();
            MetaBlock blockMeta = new MetaBlock(blockOffset, firstKey, lastKey, block.getKeyValueNum());
            storeFile.add(block, blockMeta);
            blockOffset += block.length();
            firstKey = null;
            lastKey = null;
            blockBuilder = new DataBlockBuilder();
            length = 0;
        }
    }

    public StoreFile build() {
        if (!blockBuilder.isEmpty()) {
            DataBlock block = blockBuilder.build();
            MetaBlock blockMeta = new MetaBlock(blockOffset, firstKey, lastKey, block.getKeyValueNum());
            storeFile.add(block, blockMeta);
        }
        return storeFile;
    }



}
