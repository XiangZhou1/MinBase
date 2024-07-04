package org.minbase.server.lsmStorage;


import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class TestLsmStorageInner {

    LsmStorage lsmStorageInner;

    @Before
    public void before() throws Exception{

        lsmStorageInner = new LsmStorage();
    }
//
    @Test
    public void test1(){
        lsmStorageInner.put("k1".getBytes(), "v1".getBytes());
        byte[] bytes = lsmStorageInner.get("k1".getBytes());
        System.out.println(new String(bytes));
        assert new String(bytes).equals("v1");
    }

    public static void main(String[] args) throws Exception {
        LsmStorage lsmStorageInner = new LsmStorage();

//        for (int i = 0; i < 1000; i++) {
//            lsmStorageInner.put(("k" + i).getBytes(), ("v" + i).getBytes());
//        }
//        byte[] bytes = lsmStorageInner.get("k1".getBytes());
//        System.out.println(new String(bytes));
        while (true);
        //Thread.sleep(100000000);
    }

    @Test
    public void test2() throws Exception {
        for (int i = 0; i < 1000; i++) {
            lsmStorageInner.put(("k" + i).getBytes(), ("v" + i).getBytes());
        }
        byte[] bytes = lsmStorageInner.get("k1".getBytes());
        System.out.println(new String(bytes));
        while (true);
//        Thread.sleep(100000000);
    }

    @Test
    public void test3() throws Exception {
        Random random = new Random();
        for (int i = 0; i < 100000000; i++) {
            lsmStorageInner.put(("k" + random.nextInt(1000)).getBytes(), ("v" + i).getBytes());
//            Thread.sleep(10);
//            System.out.println(i);
        }
//        byte[] bytes = lsmStorageInner.get("k1".getBytes());
//        System.out.println(new String(bytes));
        while (true);
//        Thread.sleep(100000000);
    }

    @Test
    public void test4() throws Exception {
        for (int i = 0; i < 100000000; i++) {
            final byte[] bytes = lsmStorageInner.get(("k" + i).getBytes());
            System.out.println(bytes == null?"null":new String(bytes));
            assert new String(bytes).equals("v" + i);
            Thread.sleep(10);
//            System.out.println(i);
        }
//        Thread.sleep(100000000);
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
