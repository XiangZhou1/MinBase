package org.minbase.server.storage.sstable;



import org.minbase.server.constant.Constants;
import org.minbase.server.iterator.SSTableIterator;
import org.minbase.server.op.Key;
import org.minbase.server.storage.block.BloomFilterBlock;
import org.minbase.server.storage.block.DataBlock;
import org.minbase.server.storage.block.MetaBlock;
import org.minbase.server.storage.cache.LRUBlockCache;
import org.minbase.server.utils.ByteUtils;
import org.minbase.server.utils.IOUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.UUID;

/**
 * 数据结构 SSTable
 * ---------------------------   (< ====  offset )
 * DataBlock1     | KeyValue (byte[])    (key_len + key + valueLen + value)
 * | KeyValue (byte[])
 * | .........
 * ---------------------------
 * DataBlock2     | KeyValue (byte[])
 * | KeyValue (byte[])
 * | .........
 * ---------------------------
 * ............
 * ---------------------------  (< ====  MetaBlockOffset )
 * | firstKeyLen (Integer)
 * MetaBlock1     | firstKey (byte[])
 * | lastKeyLen (Integer)
 * | lastKey (byte[])
 * | offset (Long)
 * | KeyValue_num (Integer)
 * ---------------------------
 * | firstKeyLen (Integer)
 * MetaBlock2     | firstKey (byte[])
 * | lastKeyLen (Integer)
 * | lastKey (byte[])
 * | offset (Long)
 * | KeyValue_num (Integer)
 * ---------------------------
 * ............
 * ---------------------------
 * BloomFilterBlock | bitset (Long)
 * ---------------------------
 * MetaBlockOffset  | offset(Long)
 * ---------------------------
 * SSTable_Version  | version (short)
 * ---------------------------
 **/
public class SSTable {
    // 实际物理数据结构
    private ArrayList<DataBlock> dataBlocks;
    private ArrayList<MetaBlock> metaBlocks;
    private BloomFilterBlock bloomFilter;
    private long metaBlockOffset;
    private static final short ssTableVersion = 1;

    // 整个文件的大小
    private long dataLength;
    // 整个文件的key都是有序的, 这是整个文件的起始key, 终止key
    private Key firstKey;
    private Key lastKey;

    private String filePath;
    private String ssTableId;


    public SSTable() {
        this.bloomFilter = new BloomFilterBlock();
        this.dataBlocks = new ArrayList<>();
        this.metaBlocks = new ArrayList<>();
        this.ssTableId = UUID.randomUUID().toString();
    }

    public SSTable(BloomFilterBlock bloomFilter) {
        this.bloomFilter = bloomFilter;
        this.dataBlocks = new ArrayList<>();
        this.metaBlocks = new ArrayList<>();
        this.ssTableId = UUID.randomUUID().toString();
    }

    public int numOfBlocks() {
        return metaBlocks.size();
    }

    public String getSsTableId() {
        return ssTableId;
    }

    public void add(DataBlock block, MetaBlock blockMeta) {
        dataBlocks.add(block);
        metaBlocks.add(blockMeta);
        metaBlockOffset += block.length();
        dataLength += block.length() + blockMeta.length();

        if (firstKey == null || firstKey.compareTo(blockMeta.getFirstKey()) > 0) {
            firstKey = blockMeta.getFirstKey();
        }
        if (lastKey == null || lastKey.compareTo(blockMeta.getLastKey()) < 0) {
            lastKey = blockMeta.getLastKey();
        }
    }

    private String getBlockId(int i) {
        return ssTableId + "_" + (i);
    }

    public Key getFirstKey() {
        return firstKey;
    }

    public Key getLastKey() {
        return lastKey;
    }

    public ArrayList<MetaBlock> getMetaBlocks() {
        return metaBlocks;
    }

    public void setSsTableId(String ssTableId) {
        this.ssTableId = ssTableId;
    }

    public long length() {
        return dataLength + 2 * Constants.LONG_LENGTH + Constants.SHORT_LENGTH;
    }

    public byte[] encode() {
        byte[] bytes = new byte[(int) length()];
        int index = 0;
        for (DataBlock dataBlock : dataBlocks) {
            byte[] blockBytes = dataBlock.encode();
            System.arraycopy(blockBytes, 0, bytes, index, blockBytes.length);
            index += blockBytes.length;
        }

        for (MetaBlock metaBlock : metaBlocks) {
            byte[] metaBytes = metaBlock.encode();
            System.arraycopy(metaBytes, 0, bytes, index, metaBytes.length);
            index += metaBytes.length;
        }

        System.arraycopy(bloomFilter.encode(), 0, bytes, index, Constants.LONG_LENGTH);
        index += Constants.LONG_LENGTH;

        System.arraycopy(ByteUtils.longToByteArray(metaBlockOffset), 0, bytes, index, Constants.LONG_LENGTH);
        index += Constants.LONG_LENGTH;

        System.arraycopy(ByteUtils.shotToByteArray(ssTableVersion), 0, bytes, index, Constants.SHORT_LENGTH);
        return bytes;
    }

    public DataBlock getBlock(int index, boolean cached) {
        String blockId = getBlockId(index);
        DataBlock cache = LRUBlockCache.BlockCache.get(blockId);
        if (cache != null) {
            return cache;
        }

        DataBlock block = null;
        try {
            // 还未加载
            block = loadBlockFromFile(index);
        } catch (Exception e) {
            throw new RuntimeException("read data block fail", e);
        }
        if (cached) {
            LRUBlockCache.BlockCache.put(blockId, block);
        }
        return block;
    }

    private long blockSize(int index) {
        if (index < numOfBlocks() - 1) {
            return metaBlocks.get(index + 1).getOffset() - metaBlocks.get(index).getOffset();
        } else {
            return metaBlockOffset - metaBlocks.get(index).getOffset();
        }
    }

    private DataBlock loadBlockFromFile(int index) throws IOException {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(filePath, "r")) {
            randomAccessFile.seek(metaBlocks.get(index).getOffset());

            long blockSize = blockSize(index);
            byte[] buf = IOUtils.read(randomAccessFile, blockSize);

            DataBlock block = new DataBlock();
            block.setKeyValueNum(metaBlocks.get(index).getKeyValueNum());
            block.decode(buf);
            return block;
        }
    }

    public SSTableIterator iterator(Key startKey, Key endKey) {
        return new SSTableIterator(this, startKey, endKey);
    }

    public SSTableIterator iterator() {
        return new SSTableIterator(this, null, null);
    }

    public SSTableIterator compactionIterator() {
        return new SSTableIterator(this, null, null, false);
    }


    public void loadFromFile(RandomAccessFile file) throws IOException {
        long fileLength = file.length();
        dataLength = (int) fileLength - 2 * Constants.LONG_LENGTH - Constants.SHORT_LENGTH;

        // 检查version
        file.seek(fileLength - Constants.SHORT_LENGTH);
        byte[] versionBytes = IOUtils.read(file, Constants.SHORT_LENGTH);
        if (ssTableVersion != ByteUtils.byteArrayToShort(versionBytes, 0)) {
            throw new RuntimeException("Wrong sstable version");
        }

        // 查看metaBlockOffset
        file.seek(fileLength - Constants.SHORT_LENGTH - Constants.LONG_LENGTH);
        byte[] metaBlockOffsetBytes = IOUtils.read(file, Constants.LONG_LENGTH);
        metaBlockOffset = ByteUtils.byteArrayToLong(metaBlockOffsetBytes, 0);

        // 查看bloomFilter
        file.seek(fileLength - Constants.SHORT_LENGTH - Constants.LONG_LENGTH - Constants.LONG_LENGTH);
        byte[] bloomFilterBytes = IOUtils.read(file, Constants.LONG_LENGTH);
        this.bloomFilter.decode(bloomFilterBytes);

        // 查看metaBlocks
        file.seek(metaBlockOffset);
        MetaBlock metaBlock = new MetaBlock();
        long allMetaBlocksLength = fileLength - metaBlockOffset - Constants.LONG_LENGTH * 2 - Constants.SHORT_LENGTH;
        byte[] allMetaBlocks = IOUtils.read(file, allMetaBlocksLength);

        int pos = 0;
        while (pos < allMetaBlocksLength) {
            metaBlock.decode(allMetaBlocks, pos);
            metaBlocks.add(metaBlock);
            pos += metaBlock.length();
            metaBlock = new MetaBlock();
        }

        getTableKeyRange();
    }

    private void getTableKeyRange() {
        for (MetaBlock metaBlock : metaBlocks) {
            if (firstKey == null || firstKey.compareTo(metaBlock.getFirstKey()) > 0) {
                firstKey = metaBlock.getFirstKey();
            }
            if (lastKey == null || lastKey.compareTo(metaBlock.getLastKey()) < 0) {
                lastKey = metaBlock.getLastKey();
            }
        }
    }

    public boolean inRange(Key startKey, Key endKey) {
        if (endKey.compareTo(this.firstKey) <= 0 || startKey.compareTo(this.lastKey) > 0) {
            return false;
        }
        return true;
    }

    public boolean mightContain(byte[] userKey) {
        return bloomFilter.mightContain(userKey);
    }


    public void cacheDataBlock() {
        for (int i = 0; i < dataBlocks.size(); i++) {
            DataBlock dataBlock = dataBlocks.get(i);
            String blockId = getBlockId(i);
            LRUBlockCache.BlockCache.put(blockId, dataBlock);
        }
    }

    public String getFilePath() {
        return filePath;
    }


    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public String toString() {
        return "SSTable{" +
                "dataBlocks=" + dataBlocks +
                ", metaBlocks=" + metaBlocks +
                ", bloomFilter=" + bloomFilter +
                ", metaBlockOffset=" + metaBlockOffset +
                ", dataLength=" + dataLength +
                ", firstKey=" + firstKey +
                ", lastKey=" + lastKey +
                ", filePath='" + filePath + '\'' +
                ", ssTableId='" + ssTableId + '\'' +
                '}';
    }
}
