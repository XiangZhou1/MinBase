package org.minbase.server.kv.storage.storefile;


import org.junit.Test;
import org.minbase.common.utils.ByteUtil;
import org.minbase.common.conf.Configuration;
import org.minbase.server.constant.Constants;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.storage.StoreFileIterator;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.storage.StoreFile;
import org.minbase.server.kv.storage.StoreFileBuilder;
import org.minbase.server.kv.storage.StoreFileReader;
import org.minbase.server.kv.storage.cache.BlockCache;
import org.minbase.server.kv.storage.cache.LRUBlockCache;
import org.minbase.server.kv.utils.ValueUtil;
import org.minbase.server.kv.Value;
import org.minbase.common.utils.Util;

import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.util.UUID;

public class StoreFileTest {
    private static Configuration configuration = new Configuration();
    private static final BlockCache BLOCK_CAHCE = new LRUBlockCache(256 * 1024 * 1024);

    public static void cahceStoreFile(StoreFile storeFile) {
        for (int i = 0; i < storeFile.getDataBlocks().size(); i++) {
            BLOCK_CAHCE.put(storeFile, i, storeFile.getDataBlocks().get(i));
        }
    }

    @Test
    public void testEncodeDecode() throws Exception {
        int totalNum = 4000;
        StoreFileBuilder storeFileBuilder = new StoreFileBuilder(Constants.STORE_FILE_BLOCK_LENGTH_LIMIT_DEFAULT);
        for (int i = 0; i < totalNum; i++) {
            Key key2 = new Key(("k" + Util.fillZero(i)).getBytes(), 2);
            Key key1 = new Key(("k" + Util.fillZero(i)).getBytes(), 1);
            Value put = ValueUtil.Put("v1".getBytes());
            storeFileBuilder.add(new KeyValue(key2, put));
            storeFileBuilder.add(new KeyValue(key1, put));
        }

        StoreFile storeFile = storeFileBuilder.build();
        //storeFile.cacheDataBlocks();
        storeFile.encodeToStream(new FileOutputStream("tmp"));

        StoreFile storeFile1 = new StoreFile(UUID.randomUUID().toString());
        storeFile1.decodeFromFile(new RandomAccessFile("tmp", "r"));

        assert ByteUtil.byteEqual(storeFile1.getMetaBlock(0).encode(), storeFile.getMetaBlock(0).encode());
        assert ByteUtil.byteEqual(storeFile1.getBloomFilter().encode(), storeFile.getBloomFilter().encode());

        for (int i = 0; i < totalNum; i++) {
            Key key2 = new Key(("k" + Util.fillZero(i)).getBytes(), 2);
            assert storeFile1.mightContain(key2.getInternalKey());
        }
    }

    @Test
    public void testIter() throws Exception {
        int totalNum = 4000;
        StoreFileBuilder storeFileBuilder = new StoreFileBuilder(Constants.STORE_FILE_BLOCK_LENGTH_LIMIT_DEFAULT);
        for (int i = 0; i < totalNum; i++) {
            Key key2 = new Key(("k" + Util.fillZero(i)).getBytes(), 2);
            Key key1 = new Key(("k" + Util.fillZero(i)).getBytes(), 1);
            Value put = ValueUtil.Put("v1".getBytes());
            storeFileBuilder.add(new KeyValue(key2, put));
            storeFileBuilder.add(new KeyValue(key1, put));
        }

        StoreFile storeFile = storeFileBuilder.build();
        cahceStoreFile(storeFile);
        StoreFileReader storeFileReader = storeFile.getStoreFileReader();
        storeFileReader.setBlockCache(BLOCK_CAHCE);
        StoreFileIterator iterator = storeFileReader.iterator();
        for (int i = 0; i < totalNum; i++) {
            Key key2 = new Key(("k" + Util.fillZero(i)).getBytes(), 2);
            Key key1 = new Key(("k" + Util.fillZero(i)).getBytes(), 1);
            if (iterator.hasNext()) {
                KeyValue next = iterator.next();
                assert ByteUtil.byteEqual(next.getKey().encode(), key2.encode());
            } else {
                assert false;
            }
            if (iterator.hasNext()) {
                KeyValue next = iterator.next();
                assert ByteUtil.byteEqual(next.getKey().encode(), key1.encode());
            } else {
                assert false;
            }
        }
    }


    @Test
    public void testIter2() throws Exception {
        int totalNum = 100000;
        StoreFileBuilder storeFileBuilder = new StoreFileBuilder(Constants.STORE_FILE_BLOCK_LENGTH_LIMIT_DEFAULT);
        for (int i = 0; i < totalNum; i++) {
            Key key2 = new Key(("k" + Util.fillZero(i)).getBytes(), 2);
            Key key1 = new Key(("k" + Util.fillZero(i)).getBytes(), 1);
            Value put = ValueUtil.Put("v1".getBytes());
            storeFileBuilder.add(new KeyValue(key2, put));
            storeFileBuilder.add(new KeyValue(key1, put));
        }

        StoreFile storeFile = storeFileBuilder.build();
        cahceStoreFile(storeFile);
        StoreFileReader storeFileReader = storeFile.getStoreFileReader();
        storeFileReader.setBlockCache(BLOCK_CAHCE);
        StoreFileIterator iterator = storeFileReader.iterator();
        for (int i = 0; i < totalNum; i++) {
            Key key2 = new Key(("k" + Util.fillZero(i)).getBytes(), 2);
            Key key1 = new Key(("k" + Util.fillZero(i)).getBytes(), 1);
            if (iterator.hasNext()) {
                KeyValue next = iterator.next();
                assert ByteUtil.byteEqual(next.getKey().encode(), key2.encode());
            } else {
                System.out.println(iterator.key());
                assert false;
            }
            if (iterator.hasNext()) {
                KeyValue next = iterator.next();
                assert ByteUtil.byteEqual(next.getKey().encode(), key1.encode());
            } else {
                System.out.println(iterator.key());
                assert false;
            }
        }
    }

    @Test
    public void testIter3() throws Exception {
        int totalNum = 100000;
        StoreFileBuilder storeFileBuilder = new StoreFileBuilder(Constants.STORE_FILE_BLOCK_LENGTH_LIMIT_DEFAULT);
        for (int i = 0; i < totalNum; i++) {
            Key key2 = new Key(("k" + Util.fillZero(i)).getBytes(), 2);
            Key key1 = new Key(("k" + Util.fillZero(i)).getBytes(), 1);
            Value put = ValueUtil.Put("v1".getBytes());
            storeFileBuilder.add(new KeyValue(key2, put));
            storeFileBuilder.add(new KeyValue(key1, put));
        }

        StoreFile storeFile = storeFileBuilder.build();
        cahceStoreFile(storeFile);
        StoreFileReader storeFileReader = storeFile.getStoreFileReader();
        storeFileReader.setBlockCache(BLOCK_CAHCE);
        for (int j = 0; j < totalNum - 1000; j++) {
            StoreFileIterator iterator = storeFileReader.iterator(new Key(("k" + Util.fillZero(j)).getBytes(), 2), new Key(("k" + Util.fillZero(j + 1000)).getBytes(), 2));
            for (int i = j; i < j + 1000; i++) {
                Key key2 = new Key(("k" + Util.fillZero(i)).getBytes(), 2);
                Key key1 = new Key(("k" + Util.fillZero(i)).getBytes(), 1);
                if (iterator.hasNext()) {
                    KeyValue next = iterator.next();
                    assert ByteUtil.byteEqual(next.getKey().encode(), key2.encode());
                } else {
                    System.out.println(iterator.key());
                    assert false;
                }
                if (iterator.hasNext()) {
                    KeyValue next = iterator.next();
                    assert ByteUtil.byteEqual(next.getKey().encode(), key1.encode());
                } else {
                    System.out.println(iterator.key());
                    assert false;
                }
            }
        }
    }
}
