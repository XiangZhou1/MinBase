package org.minbase.server.storage.sstable;


import org.minbase.server.conf.Config;
import org.minbase.server.constant.Constants;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.storage.block.BloomFilterBlock;
import org.minbase.server.storage.block.DataBlock;
import org.minbase.server.storage.block.DataBlockBuilder;
import org.minbase.server.storage.block.MetaBlock;
import org.minbase.server.utils.Utils;

public class SSTBuilder {
    public int MAX_BLOCK_SIZE = (int) Utils.parseUnit(Config.get(Constants.KEY_MAX_BLOCK_SIZE));
    SSTable ssTable;

    Key firstKey;
    Key lastKey;
    int blockOffset;
    int length = 0;
    BloomFilterBlock bloomFilter;
    DataBlockBuilder blockBuilder;


    public SSTBuilder() {
        blockBuilder = new DataBlockBuilder();
        bloomFilter = new BloomFilterBlock();
        ssTable = new SSTable(bloomFilter);
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
            ssTable.add(block, blockMeta);
            blockOffset += block.length();
            firstKey = null;
            lastKey = null;
            blockBuilder = new DataBlockBuilder();
            length = 0;
        }
    }

    public SSTable build() {
        if (!blockBuilder.isEmpty()) {
            DataBlock block = blockBuilder.build();
            MetaBlock blockMeta = new MetaBlock(blockOffset, firstKey, lastKey, block.getKeyValueNum());
            ssTable.add(block, blockMeta);
        }
        return ssTable;
    }

}
