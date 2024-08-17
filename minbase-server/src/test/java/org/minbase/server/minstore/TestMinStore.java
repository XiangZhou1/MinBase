package org.minbase.server.minstore;


import org.junit.Before;
import org.junit.Test;
import org.minbase.common.op.Put;
import org.minbase.server.compaction.CompactThread;
import org.minbase.server.compaction.Compaction;
import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.kv.KeyImpl;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.transaction.store.WriteBatch;
import org.minbase.server.utils.KeyValueUtil;
import org.mockito.Mockito;

import java.io.File;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class TestMinStore {
    private static final byte[] key1 = "key1".getBytes();
    private static final byte[] column1 = "column1".getBytes();
    private static final byte[] value1 = "value1".getBytes();
    private static final String tableName = "table1";
    MinStore minStore;

    @Before
    public void before() throws Exception {
        String name = "table1";
        File dir = null;
        Executor flushThread = Executors.newCachedThreadPool();
        Compaction compaction = Mockito.mock(Compaction.class);
        Mockito.doReturn(false).when(compaction.needCompact(Mockito.any()));
        CompactThread compactThread = new CompactThread(compaction, null);
        minStore = new MinStore(name, dir, flushThread, compaction, compactThread);
    }
//
    @Test
    public void test1() {
        Put put = new Put(key1, column1, value1);
        WriteBatch writeBatch = new WriteBatch();
        writeBatch.add(tableName, KeyValueUtil.toKeyValue(put));
        writeBatch.setSequenceId(1);
        minStore.put(writeBatch);

        KeyValueIterator iterator = minStore.iterator(KeyImpl.minKey(key1), KeyImpl.maxKey(key1));
        while (iterator.isValid()) {
            KeyValue value = iterator.value();
            System.out.println(value);
            iterator.next();
        }
    }



    @Test
    public void test2() throws Exception {
        for (long i = 0; i < 1000000000; i++) {
            Put put = new Put(("k" + i).getBytes(), column1, ("v" + i).getBytes());
            WriteBatch writeBatch = new WriteBatch();
            writeBatch.add(tableName, KeyValueUtil.toKeyValue(put));
            writeBatch.setSequenceId(i);
            minStore.put(writeBatch);
        }

        KeyValueIterator iterator = minStore.iterator(KeyImpl.minKey(key1), KeyImpl.maxKey(key1));
        while (iterator.isValid()) {
            KeyValue value = iterator.value();
            System.out.println(value);
            iterator.next();
        }
    }

//
//    @Test
//    public void testIter(){
//        lsmStorageInner.put("k1".getBytes(), "v1".getBytes());
//        lsmStorageInner.put("k2".getBytes(), "v2".getBytes());
//        lsmStorageInner.put("k3".getBytes(), "v3".getBytes());
//        lsmStorageInner.delete("k1".getBytes());
//        byte[] bytes = lsmStorageInner.get("k1".getBytes());
//        lsmStorageInner.put("k30".getBytes(), "v3".getBytes());
//        lsmStorageInner.put("k29".getBytes(), "v3".getBytes());
//        lsmStorageInner.put("k28".getBytes(), "v3".getBytes());
//        lsmStorageInner.put("k33".getBytes(), "v3".getBytes());
//        lsmStorageInner.put("k25".getBytes(), "v3".getBytes());
//        lsmStorageInner.put("k20".getBytes(), "v3".getBytes());
//        lsmStorageInner.put("k21".getBytes(), "v3".getBytes());
//        lsmStorageInner.put("k18".getBytes(), "v3".getBytes());
//        lsmStorageInner.put("k15".getBytes(), "v3".getBytes());
//        System.out.println(bytes == null);
//
//        LsmIterator iterator = lsmStorageInner.iterator();
//        while (iterator.isValid()){
//            System.out.println(new String(iterator.key()) + ":" + new String(iterator.value()));
//            iterator.next();
//        }
//    }
//
//    @Test
//    public void testScan(){
//        lsmStorageInner.put("k1".getBytes(), "v1".getBytes());
//        lsmStorageInner.put("k2".getBytes(), "v2".getBytes());
//        lsmStorageInner.put("k3".getBytes(), "v3".getBytes());
//        lsmStorageInner.delete("k1".getBytes());
//        byte[] bytes = lsmStorageInner.get("k1".getBytes());
//        lsmStorageInner.put("k30".getBytes(), "v3".getBytes());
//        lsmStorageInner.put("k29".getBytes(), "v3".getBytes());
//        lsmStorageInner.put("k28".getBytes(), "v3".getBytes());
//        lsmStorageInner.put("k33".getBytes(), "v3".getBytes());
//        lsmStorageInner.put("k25".getBytes(), "v3".getBytes());
//        lsmStorageInner.put("k20".getBytes(), "v3".getBytes());
//        lsmStorageInner.put("k21".getBytes(), "v3".getBytes());
//        lsmStorageInner.put("k18".getBytes(), "v3".getBytes());
//        lsmStorageInner.put("k15".getBytes(), "v3".getBytes());
//        System.out.println(bytes == null);
//
//        LsmIterator iterator = lsmStorageInner.scan("k2".getBytes(), null);
//        while (iterator.isValid()){
//            System.out.println(new String(iterator.key()) + ":" + new String(iterator.value()));
//            iterator.next();
//        }
//    }
//
//    @Test
//    public void testScan2(){
//        for (int i = 0; i < 10000; i++) {
//            lsmStorageInner.put(("k" + i).getBytes(), ("v" + i).getBytes());
//        }
//
//        LsmIterator iterator = lsmStorageInner.scan("k200".getBytes(), null);
//        while (iterator.isValid()){
//            System.out.println(new String(iterator.key()) + ":" + new String(iterator.value()));
//            iterator.next();
//        }
//    }
//
//
//    @Test
//    public void testGet() throws InterruptedException {
//        for (int i = 0; i < 10000; i++) {
//            lsmStorageInner.put(("k" + i).getBytes(), ("v" + i).getBytes());
//        }
//
////        byte[] k200s = lsmStorageInner.get("k200".getBytes());
////        System.out.println(new String(k200s));
//        while (true){
//            Thread.sleep(10000);
//        }
//    }
//
//    @Test
//    public void testBloomFilter() throws InterruptedException {
//        BloomFilterBlock bloomFilter = new BloomFilterBlock();
//        byte[] encode = bloomFilter.encode();
//        System.out.println(encode.length);
//    }
//
//    public static void main(String[] args) throws Exception {
//
//
//        LsmStorage lsmStorageInner;
//        options = LsmStorageOptions.builder()
//                .memtableSizeLimit(100).build();
//
//        lsmStorageInner = new LsmStorage(options);
//        for (int i = 0; i < 10000; i++) {
//            lsmStorageInner.put(("k" + i).getBytes(), ("v" + i).getBytes());
//        }
//
////        byte[] k200s = lsmStorageInner.get("k200".getBytes());
////        System.out.println(new String(k200s));
//        while (true){
//            Thread.sleep(10000);
//        }
//    }
}
