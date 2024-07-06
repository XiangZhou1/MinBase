package org.minbase.server.lsmStorage;

import org.junit.Before;
import org.junit.Test;
import org.minbase.server.iterator.KeyIterator;
import org.minbase.server.op.WriteBatch;
import org.minbase.server.transaction.writeBatch.WriteBatchTableIterator;
import org.minbase.server.utils.ByteUtils;

import java.util.Scanner;

public class StorageManagerTest {
    LsmStorage lsmStorage;

    @Before
    public void before() throws Exception{
        lsmStorage = new LsmStorage();
    }


    // 将compaction关闭
    @Test
    public void test1() throws Exception {
        long num = 500000;
        for (long i = 0; i < num; i++) {
            lsmStorage.put(("k" + i).getBytes(), ("v" + i).getBytes());
            if (i % 1000 == 0) {
                System.out.println("k" + i);
            }

//            Thread.sleep(1);
        }
        Thread.sleep(30 * 1000);
        for (long i = 0; i < num; i++) {
            byte[] bytes = lsmStorage.get(("k" + i).getBytes());
            System.out.println(("k" + i) +":"+ (bytes == null ? "null" : new String(bytes)));
            if(bytes == null){
                throw new RuntimeException(("k" + i) +":"+"null");
            }else{
                assert new String(bytes).equals(("v" + i));
            }
//            Thread.sleep(1);
        }

        Scanner scanner = new Scanner(System.in);
        scanner.next();
    }

    // 将compaction关闭
    @Test
    public void test2() throws Exception {
        long num = 500000;

        for (long i = 0; i < num; i++) {
            byte[] bytes = lsmStorage.get(("k" + i).getBytes());

            System.out.println(("k" + i) +":"+ (bytes == null ? "null" : new String(bytes)));
            if(bytes == null){
                throw new RuntimeException(("k" + i) +":"+"null");
            }else{
                assert new String(bytes).equals(("v" + i));
            }
//            Thread.sleep(1);
        }

        Scanner scanner = new Scanner(System.in);
        scanner.next();
    }


    // 将compaction关闭
    @Test
    public void test3() throws Exception {
        long num = 500000;

        for (long i = 0; i < num; i++) {
            WriteBatch writeBatch = new WriteBatch();
            writeBatch.put(ByteUtils.toBytes("k" + i), ByteUtils.toBytes("v" + i));
            writeBatch.put(ByteUtils.toBytes("k_" + i), ByteUtils.toBytes("v_" + i));
            lsmStorage.put(writeBatch);
            if (i % 1000 == 0) {
                System.out.println(i);
            }
        }

        System.out.println("beforeFlush");
        lsmStorage.foreFlush();
        System.out.println("afterFlush");
        //Thread.sleep(10 * 1000);
        int totalNum = 0;
        final KeyIterator iterator = lsmStorage.iterator();
        while (iterator.isValid()){
            System.out.println(iterator.value());
            iterator.nextUserKey();
            totalNum ++;
        }
        iterator.close();

        System.out.println(totalNum);
        Scanner scanner = new Scanner(System.in);
        scanner.next();
    }


    @Test
    public void test4() throws Exception {
        long num = 500000;


        Scanner scanner = new Scanner(System.in);
        scanner.next();
    }


}
