package org.minbase.server.kv.storage;


import org.minbase.server.constant.Constants;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.storage.block.BloomFilterBlock;
import org.minbase.server.kv.storage.block.DataBlock;
import org.minbase.server.kv.storage.block.MetaBlock;
import org.minbase.common.utils.ByteUtil;
import org.minbase.common.utils.FileUtil;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;

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
 * BloomFilterBlock | length (Long)
 * ---------------------------
 * MetaBlockOffset  | offset(Long)
 * ---------------------------
 * VERSION  | version (short)
 * ---------------------------
 **/
public class StoreFile {
    public static final short STORE_FILE_VERSION = 1;

    // 实际物理数据结构
    private ArrayList<DataBlock> dataBlocks;
    private ArrayList<MetaBlock> metaBlocks;
    private BloomFilterBlock bloomFilter;
    private long metaBlockOffset;

    // 整个文件的大小
    private long dataLength;
    // 整个文件的key都是有序的, 这是整个文件的起始key, 终止key
    private Key firstKey;
    private Key lastKey;

    private String storeId;

    private StoreFileReader reader;

    /**
     * 原始文件
     */
    private File rawFile;

    public File getRawFile() {
        return rawFile;
    }

    public StoreFileReader getStoreFileReader() {
        if (reader == null) {
            synchronized (this) {
                if (reader == null) {
                    reader = new StoreFileReader(this);
                }
            }
        }
        return reader;
    }

    public StoreFile(String storeIdId) {
        this.storeId = storeIdId;
        this.bloomFilter = new BloomFilterBlock();
        this.dataBlocks = new ArrayList<>();
        this.metaBlocks = new ArrayList<>();
    }

    public StoreFile(String storeIdId, BloomFilterBlock bloomFilter) {
        this.storeId = storeIdId;
        this.bloomFilter = bloomFilter;
        this.dataBlocks = new ArrayList<>();
        this.metaBlocks = new ArrayList<>();
    }

    public int numOfBlocks() {
        return metaBlocks.size();
    }

    public String getStoreId() {
        return StoreFile.this.storeId;
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

    public String getBlockId(int i) {
        return StoreFile.this.storeId + "_" + i;
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

    public MetaBlock getMetaBlock(int index) {
        return metaBlocks.get(index);
    }

    public void setStoreId(String storeId) {
        this.storeId = storeId;
    }

    public long length() {
        return dataLength + 2 * Constants.LONG_LENGTH + Constants.SHORT_LENGTH;
    }

    public long getBlockSize(int index) {
        if (index < numOfBlocks() - 1) {
            return metaBlocks.get(index + 1).getOffset() - metaBlocks.get(index).getOffset();
        } else {
            return metaBlockOffset - metaBlocks.get(index).getOffset();
        }
    }

    public void setRawFile(File rawFile) {
        this.rawFile = rawFile;
    }


    public void decodeFromFile(RandomAccessFile file) throws IOException {
        long fileLength = file.length();
        dataLength = (int) fileLength - 2 * Constants.LONG_LENGTH - Constants.SHORT_LENGTH;

        // 检查version
        file.seek(fileLength - Constants.SHORT_LENGTH);
        byte[] versionBytes = FileUtil.read(file, Constants.SHORT_LENGTH);
        if (STORE_FILE_VERSION != ByteUtil.byteArrayToShort(versionBytes, 0)) {
            throw new RuntimeException("Wrong storFile version, expected version=" + STORE_FILE_VERSION);
        }

        // 查看metaBlockOffset
        file.seek(fileLength - Constants.SHORT_LENGTH - Constants.LONG_LENGTH);
        byte[] metaBlockOffsetBytes = FileUtil.read(file, Constants.LONG_LENGTH);
        metaBlockOffset = ByteUtil.byteArrayToLong(metaBlockOffsetBytes, 0);

        // 查看bloomFilter
        file.seek(fileLength - Constants.SHORT_LENGTH - Constants.LONG_LENGTH - Constants.LONG_LENGTH);
        byte[] bloomFilterBlockLengthBytes = FileUtil.read(file, Constants.LONG_LENGTH);
        long bloomFilterBlockLength = ByteUtil.byteArrayToLong(bloomFilterBlockLengthBytes, 0);
        file.seek(fileLength - Constants.SHORT_LENGTH - Constants.LONG_LENGTH -
                Constants.LONG_LENGTH - bloomFilterBlockLength);
        byte[] bloomFilterBytes = FileUtil.read(file, bloomFilterBlockLength);
        this.bloomFilter.decode(bloomFilterBytes);

        // 查看metaBlocks
        file.seek(metaBlockOffset);
        MetaBlock metaBlock = new MetaBlock();
        long allMetaBlocksLength = fileLength - metaBlockOffset - Constants.LONG_LENGTH * 2 - Constants.SHORT_LENGTH
                - bloomFilterBlockLength;
        byte[] allMetaBlocks = FileUtil.read(file, allMetaBlocksLength);

        int pos = 0;
        while (pos < allMetaBlocksLength) {
            metaBlock.decode(allMetaBlocks, pos);
            metaBlocks.add(metaBlock);
            pos += metaBlock.length();
            metaBlock = new MetaBlock();
        }

        setKeyRange();
    }


    public int encodeToStream(OutputStream outputStream) throws IOException {
        int index = 0;
        for (DataBlock dataBlock : dataBlocks) {
            int dataBlockLen = dataBlock.encodeToStream(outputStream);
            index += dataBlockLen;
        }

        for (MetaBlock metaBlock : metaBlocks) {
            int metaBlockLen = metaBlock.encodeToStream(outputStream);
            index += metaBlockLen;
        }

        byte[] bloomFilterBytes = bloomFilter.encode();
        outputStream.write(bloomFilterBytes);
        index += bloomFilterBytes.length;
        outputStream.write(ByteUtil.longToByteArray(bloomFilterBytes.length));
        index += Constants.LONG_LENGTH;

        outputStream.write(ByteUtil.longToByteArray(metaBlockOffset));
        index += Constants.LONG_LENGTH;

        outputStream.write(ByteUtil.shotToByteArray(STORE_FILE_VERSION));
        index += Constants.SHORT_LENGTH;
        return index;
    }

    private void setKeyRange() {
        for (MetaBlock metaBlock : metaBlocks) {
            if (firstKey == null || firstKey.compareTo(metaBlock.getFirstKey()) > 0) {
                firstKey = metaBlock.getFirstKey();
            }
            if (lastKey == null || lastKey.compareTo(metaBlock.getLastKey()) < 0) {
                lastKey = metaBlock.getLastKey();
            }
        }
    }

    public boolean inRange(byte[] startKey, byte[] endKey, boolean isCloseInterval) {
        if (isCloseInterval) {
            return inRangeClosed(startKey, endKey);
        } else {
            return inRangeOpen(startKey, endKey);
        }
    }

    // [startKey, endKey)
    // [firstKey, lastKey]
    private boolean inRangeOpen(byte[] startKey, byte[] endKey) {
        if (startKey == null || endKey == null) {
            return true;
        } else if (startKey == null && endKey != null) {
            if (ByteUtil.byteLessOrEqual(endKey, this.firstKey.getInternalKey())) {
                return false;
            }
        } else if (startKey != null && endKey == null) {
            if (ByteUtil.byteGreater(startKey, this.lastKey.getInternalKey())) {
                return false;
            }
        } else {
            if (ByteUtil.byteLessOrEqual(endKey, this.firstKey.getInternalKey()) ||
                    ByteUtil.byteGreater(startKey, this.lastKey.getInternalKey())) {
                return false;
            }
        }
        return true;
    }

    // [startKey, endKey]
    // [firstKey, lastKey]
    private boolean inRangeClosed(byte[] startKey, byte[] endKey) {
        if (startKey == null || endKey == null) {
            return true;
        }

        if (startKey == null && endKey != null) {
            if (ByteUtil.byteLess(endKey, this.firstKey.getInternalKey())) {
                return false;
            }
        } else if (startKey != null && endKey == null) {
            if (ByteUtil.byteGreater(startKey, this.lastKey.getInternalKey())) {
                return false;
            }
        } else {
            if (ByteUtil.byteLess(endKey, this.firstKey.getInternalKey())
                    || ByteUtil.byteGreater(startKey, this.lastKey.getInternalKey())) {
                return false;
            }
        }

        return true;
    }

    public boolean mightContain(byte[] userKey) {
        return bloomFilter.contains(userKey);
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
                ", storeIdId='" + storeId + '\'' +
                '}';
    }

    public ArrayList<DataBlock> getDataBlocks() {
        return dataBlocks;
    }

    public BloomFilterBlock getBloomFilter() {
        return bloomFilter;
    }
}
