package org.minbase.server.storage.store;


import org.junit.Test;
import org.minbase.server.iterator.StoreFileIterator;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.op.Value;
import org.minbase.common.utils.Util;

import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class StoreFileTest {
    private static final byte[] column = "cl1".getBytes(StandardCharsets.UTF_8);
    @Test
    public void test1() {
        int totalNum = 4000;
        StoreFileBuilder storeFileBuilder = new StoreFileBuilder();
        for (int i = 0; i < totalNum; i++) {
            Key key = new Key(("k" + Util.fillZero(i)).getBytes(), 1);
            Value put = Value.Put(column, "v1".getBytes());
            storeFileBuilder.add(new KeyValue(key, put));
        }

        StoreFile storeFile = storeFileBuilder.build();
        storeFile.cacheDataBlocks();

        int scanNum = 0;
        StoreFileIterator iterator = storeFile.getReader().iterator();
        while (iterator.isValid()){
            KeyValue value = iterator.value();
            scanNum ++;
            System.out.println(value);
            iterator.nextInnerKey();
        }
        assert totalNum == scanNum;
    }

    @Test
    public void test2() {
        int totalNum = 400000;
        StoreFileBuilder storeFileBuilder = new StoreFileBuilder();
        for (int i = 0; i < totalNum; i++) {
            Key key2 = new Key(("k" + Util.fillZero(i)).getBytes(), 2);
            Key key1 = new Key(("k" + Util.fillZero(i)).getBytes(), 1);
            Value put = Value.Put(column, "v1".getBytes());
            storeFileBuilder.add(new KeyValue(key2, put));
            storeFileBuilder.add(new KeyValue(key1, put));
        }

        StoreFile storeFile = storeFileBuilder.build();
        storeFile.cacheDataBlocks();

        int scanNum = 0;
        StoreFileIterator iterator = storeFile.getReader().iterator();
        while (iterator.isValid()){
            KeyValue value = iterator.value();
            scanNum ++;
            System.out.println(value);
            iterator.next();
        }
        System.out.println(scanNum);
        assert totalNum == scanNum;
    }

    @Test
    public void testSeek() {
        StoreFileBuilder storeFileBuilder = new StoreFileBuilder();
        for (int i = 0; i < 40960; i++) {
            Key key = new Key(("k" + Util.fillZero(i)).getBytes(), 1);
            Value put = Value.Put(column, "v1".getBytes());
            storeFileBuilder.add(new KeyValue(key, put));
        }
        StoreFile storeFile = storeFileBuilder.build();

        for (int i = 0; i < 40960; i++) {
            StoreFileIterator iterator = storeFile.getReader().iterator(Key.latestKey(("k" + Util.fillZero(i)).getBytes()), null);
            int num = 0;
            while (iterator.isValid()) {
                KeyValue value = iterator.value();
                num++;
                iterator.nextInnerKey();
            }
            System.out.println("num=" + num +", i=" + i);
            assert num == 40960 - i;
        }
    }


    @Test
    public void testEncodeAndSave() throws Exception {
        StoreFileBuilder storeFileBuilder = new StoreFileBuilder();
        for (int i = 0; i < 4096; i++) {
            Key key = new Key(("k" + Util.fillZero(i)).getBytes(), 1);
            Value put = Value.Put(column, "v1".getBytes());
            storeFileBuilder.add(new KeyValue(key, put));
        }

        StoreFile storeFile = storeFileBuilder.build();
        StoreFileIterator iterator = storeFile.getReader().iterator();
        while (iterator.isValid()) {
            KeyValue value = iterator.value();
            //System.out.println(value);
            iterator.nextInnerKey();
        }
        try (FileOutputStream fileOutputStream = new FileOutputStream("temp")) {
            int len = storeFile.encodeToFile(fileOutputStream);
        }

        StoreFile storeFile1 = new StoreFile(UUID.randomUUID().toString());

        try (RandomAccessFile randomAccessFile = new RandomAccessFile("temp", "r")) {
            System.out.println(randomAccessFile.length());
            //byte[] read = IOUtils.read(randomAccessFile, randomAccessFile.length());
            storeFile1.decodeFromFile(randomAccessFile);
        }

        System.out.println("done");

    }





    @Test
    public void testEncodeAndSave2() throws Exception {
        StoreFileBuilder storeFileBuilder = new StoreFileBuilder();
        for (int i = 0; i < 4096; i++) {
            Key key = new Key(("k" + Util.fillZero(i)).getBytes(), 1);
            Value put = Value.Put(column, "v1".getBytes());
            storeFileBuilder.add(new KeyValue(key, put));
        }
        StoreFile storeFile = storeFileBuilder.build();
        System.out.println(storeFile);

        try (FileOutputStream fileOutputStream = new FileOutputStream("temp")) {
            int len = storeFile.encodeToFile(fileOutputStream);
        }

        StoreFile storeFile1 = new StoreFile(UUID.randomUUID().toString());

        try (RandomAccessFile randomAccessFile = new RandomAccessFile("temp", "r")) {
            System.out.println(randomAccessFile.length());
            //byte[] read = IOUtils.read(randomAccessFile, randomAccessFile.length());
            storeFile1.decodeFromFile(randomAccessFile);
        }
        System.out.println(storeFile1);

        System.out.println("done");



//        SSTableIterator iterator = ssTable.iterator();
//        while (iterator.isValid()){
//            KeyValue value = iterator.value();
//            //System.out.println(value);
//            iterator.next();
//        }

    }
//
//    @Test
//    public void testBlockMeta() throws Exception {
//        MetaBlock blockMeta = new MetaBlock();
//        blockMeta.setFirstKey("k1".getBytes());
//        blockMeta.setLastKey("k234e".getBytes());
//        blockMeta.setOffset(1);
//
//        MetaBlock blockMeta1 = new MetaBlock();
//        blockMeta1.decode(blockMeta.encode(), 0);
//        System.out.println(blockMeta1.size() == blockMeta.size());
//        System.out.println(new String(blockMeta1.getFirstKey()));
//        System.out.println(new String(blockMeta1.getLastKey()));
//        System.out.println(blockMeta1.getOffset());
//
//    }
//
//    @Test
//    public void testEncodeAndSave2() throws Exception {
//        RandomAccessFile file = new RandomAccessFile("data/level_0/1", "r");
//        SSTable ssTable = new SSTable();
//        ssTable.setLevel(0);
//        ssTable.setSsTableId(1);
//        ssTable.loadFromFile(file);
//        file.close();
//
//        SSTableIterator iterator = ssTable.iterator();
//
//    }
}
