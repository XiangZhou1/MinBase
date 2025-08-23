package org.minbase.server.kv.store;

import org.junit.Test;
import org.minbase.common.utils.ByteUtil;
import org.minbase.common.utils.Util;
import org.minbase.server.conf.Configuration;
import org.minbase.server.constant.Constants;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.Value;
import org.minbase.server.kv.storage.StoreFile;
import org.minbase.server.kv.storage.StoreFileBuilder;
import org.minbase.server.kv.storage.StoreFileManager;
import org.minbase.server.kv.storage.StoreFilesIterator;
import org.minbase.server.kv.utils.ValueUtil;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import java.io.File;

public class StoreFileManagerTest {
    public static final File STORE_DIR = new File("data1/store1");
    public static StoreManager storeManager;
    public static Store store;

    static {
        storeManager = Mockito.mock(StoreManager.class);
        PowerMockito.when(storeManager.getMinReadPoint()).thenReturn(Long.MAX_VALUE);
        store = Mockito.mock(Store.class);
    }

    @Test
    public void before() {
        File storeDir = new File(new Configuration().get(Constants.STORE_DIR_KEY, Constants.STORE_DIR_DEFAULT));
        storeDir.deleteOnExit();
    }

    @Test
    public void testCreateStoreFileManager() throws Exception {
        StoreFileManager storeFileManager = new StoreFileManager(STORE_DIR, store, new Configuration());
    }

    @Test
    public void testWriteFile() throws Exception {
        StoreFileManager storeFileManager = new StoreFileManager(STORE_DIR, store, new Configuration());
        int index = 0;
        for (int j = 0; j < 10; j++) {
            StoreFileBuilder storeFileBuilder = storeFileManager.newTmpStroeFile();
            int totalNum = 400;
            for (int i = index; i < index + totalNum; i++) {
                Key key2 = new Key(("k" + Util.fillZero(i)).getBytes(), 2);
                Key key1 = new Key(("k" + Util.fillZero(i)).getBytes(), 1);
                Value put = ValueUtil.Put("v1".getBytes());
                storeFileBuilder.add(new KeyValue(key2, put));
                storeFileBuilder.add(new KeyValue(key1, put));
            }
            index += totalNum;
            StoreFile storeFile = storeFileManager.completeStoreFile(storeFileBuilder);
        }
    }

    @Test
    public void testLoadile() throws Exception {
        StoreFileManager storeFileManager = new StoreFileManager(STORE_DIR, store, new Configuration());
        StoreFilesIterator storeFilesIterator = storeFileManager.newStoreFilesIterator(null, null, false);
        int totalNum = 400 * 10;
        for (int i = 0; i < totalNum; i++) {
            Key key2 = new Key(("k" + Util.fillZero(i)).getBytes(), 2);
            Key key1 = new Key(("k" + Util.fillZero(i)).getBytes(), 1);
            if (storeFilesIterator.hasNext()) {
                assert ByteUtil.byteEqual(storeFilesIterator.next().getKey().encode(), key2.encode());
            } else {
                System.out.println(storeFilesIterator.next().getKey());
                assert false;
            }
            if (storeFilesIterator.hasNext()) {
                assert ByteUtil.byteEqual(storeFilesIterator.next().getKey().encode(), key1.encode());
            } else {
                System.out.println(storeFilesIterator.next().getKey());
                assert false;
            }
        }
    }

    @Test
    public void testLoadileAndSeek() throws Exception {
        StoreFileManager storeFileManager = new StoreFileManager(STORE_DIR, store, new Configuration());
        int totalNum = 400 * 10;
        for (int j = 0; j < totalNum - 1000; j++) {
            System.out.println(j);
            StoreFilesIterator storeFilesIterator = storeFileManager.newStoreFilesIterator(
                    new Key(("k" + Util.fillZero(j)).getBytes(), 2), null, true);

            for (int i = j; i < 1000; i++) {
                Key key2 = new Key(("k" + Util.fillZero(i)).getBytes(), 2);
                Key key1 = new Key(("k" + Util.fillZero(i)).getBytes(), 1);
                if (storeFilesIterator.hasNext()) {
                    assert ByteUtil.byteEqual(storeFilesIterator.next().getKey().encode(), key2.encode());
                } else {
                    System.out.println(storeFilesIterator.next().getKey());
                    assert false;
                }
                if (storeFilesIterator.hasNext()) {
                    assert ByteUtil.byteEqual(storeFilesIterator.next().getKey().encode(), key1.encode());
                } else {
                    System.out.println(storeFilesIterator.next().getKey());
                    assert false;
                }
            }
        }
    }

    @Test
    public void testLoadileAndSeek2() throws Exception {
        StoreFileManager storeFileManager = new StoreFileManager(STORE_DIR, store, new Configuration());
        int totalNum = 400 * 10;
        for (int j = 0; j < totalNum - 1000; j++) {
            System.out.println(j);
            StoreFilesIterator storeFilesIterator = storeFileManager.newStoreFilesIterator(
                    new Key(("k" + Util.fillZero(j)).getBytes(), 2), new Key(("k" + Util.fillZero(j + 1000)).getBytes(), 2), false);

            for (int i = j; i < 1000; i++) {
                Key key2 = new Key(("k" + Util.fillZero(i)).getBytes(), 2);
                Key key1 = new Key(("k" + Util.fillZero(i)).getBytes(), 1);
                if (storeFilesIterator.hasNext()) {
                    assert ByteUtil.byteEqual(storeFilesIterator.next().getKey().encode(), key2.encode());
                } else {
                    System.out.println(storeFilesIterator.next().getKey());
                    assert false;
                }
                if (storeFilesIterator.hasNext()) {
                    assert ByteUtil.byteEqual(storeFilesIterator.next().getKey().encode(), key1.encode());
                } else {
                    System.out.println(storeFilesIterator.next().getKey());
                    assert false;
                }
            }
        }
    }

    @Test
    public void testCompaction() throws Exception {
        StoreFileManager storeFileManager = new StoreFileManager(STORE_DIR, store, new Configuration());
        int totalNum = 400 * 10;
        StoreFilesIterator storeFilesIterator = storeFileManager.newStoreFilesIterator(
                new Key(("k" + Util.fillZero(0)).getBytes(), 2), null, false);
        Scanner scanner = new Scanner(storeFilesIterator, Long.MAX_VALUE);
        for (int i = 0; i < 4000; i++) {
            Key key2 = new Key(("k" + Util.fillZero(i)).getBytes(), 2);

            if (scanner.hasNext()) {
                assert ByteUtil.byteEqual(scanner.next().getKey().encode(), key2.encode());
                System.out.println(scanner.key());
            } else {
                System.out.println(scanner.next().getKey());
                assert false;
            }
        }

        while (scanner.hasNext()) {
            System.out.println(scanner.next());
        }
    }
//
//    // 将compaction关闭
//    @Test
//    public void test1() throws Exception {
//        long num = 500000;
//        for (long i = 0; i < num; i++) {
//            minStore.put(("k" + i).getBytes(), ("v" + i).getBytes());
//            if (i % 1000 == 0) {
//                System.out.println("k" + i);
//            }
//
////            Thread.sleep(1);
//        }
//        Thread.sleep(30 * 1000);
//        for (long i = 0; i < num; i++) {
//            byte[] bytes = minStore.get(("k" + i).getBytes());
//            System.out.println(("k" + i) +":"+ (bytes == null ? "null" : new String(bytes)));
//            if(bytes == null){
//                throw new RuntimeException(("k" + i) +":"+"null");
//            }else{
//                assert new String(bytes).equals(("v" + i));
//            }
////            Thread.sleep(1);
//        }
//
//        Scanner scanner = new Scanner(System.in);
//        scanner.next();
//    }
//
//    // 将compaction关闭
//    @Test
//    public void test2() throws Exception {
//        long num = 500000;
//
//        for (long i = 0; i < num; i++) {
//            byte[] bytes = minStore.get(("k" + i).getBytes());
//
//            System.out.println(("k" + i) +":"+ (bytes == null ? "null" : new String(bytes)));
//            if(bytes == null){
//                throw new RuntimeException(("k" + i) +":"+"null");
//            }else{
//                assert new String(bytes).equals(("v" + i));
//            }
////            Thread.sleep(1);
//        }
//
//        Scanner scanner = new Scanner(System.in);
//        scanner.next();
//    }
//
//
//    // 将compaction关闭
//    @Test
//    public void test3() throws Exception {
//        long num = 500000;
//
//        for (long i = 0; i < num; i++) {
//            WriteBatch writeBatch = new WriteBatch();
//            writeBatch.put(ByteUtil.toBytes("k" + i), ByteUtil.toBytes("v" + i));
//            writeBatch.put(ByteUtil.toBytes("k_" + i), ByteUtil.toBytes("v_" + i));
//            minStore.put(writeBatch);
//            if (i % 1000 == 0) {
//                System.out.println(i);
//            }
//        }
//
//        System.out.println("beforeFlush");
//        minStore.foreFlush();
//        System.out.println("afterFlush");
//        //Thread.sleep(10 * 1000);
//        int totalNum = 0;
//        final KeyValueIterator iterator = minStore.iterator();
//        while (iterator.isValid()){
//            System.out.println(iterator.value());
//            iterator.next();
//            totalNum ++;
//        }
//        iterator.close();
//
//        System.out.println(totalNum);
//        Scanner scanner = new Scanner(System.in);
//        scanner.next();
//    }
//
//
//    @Test
//    public void test4() throws Exception {
//        long num = 500000;
//
//
//        Scanner scanner = new Scanner(System.in);
//        scanner.next();
//    }


}
