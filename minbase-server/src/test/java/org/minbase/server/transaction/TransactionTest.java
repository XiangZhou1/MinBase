package org.minbase.server.transaction;


import org.junit.Before;
import org.junit.Test;
import org.minbase.common.operation.Put;
import org.minbase.common.table.Table;
import org.minbase.server.MinBaseServer;
import org.minbase.server.compaction.CompactThread;
import org.minbase.server.compaction.Compaction;
import org.minbase.server.minstore.MinStore;
import org.minbase.server.op.KeyValue;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.table.TableImpl;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class TransactionTest {
    private static final Logger logger = LoggerFactory.getLogger(TransactionTest.class);
    private static final byte[] key1 = "key1".getBytes();
    private static final byte[] column1 = "column1".getBytes();
    private static final byte[] value1 = "value1".getBytes();
    private static final String tableName = "table1";

    Transaction transaction;

    public static MinStore createMinStore() throws Exception {

        File dir = null;
        Executor flushThread = Executors.newCachedThreadPool();
        Compaction compaction = Mockito.mock(Compaction.class);
        Mockito.doReturn(false).when(compaction.needCompact(Mockito.any()));
        CompactThread compactThread = new CompactThread(compaction, null);
        MinStore minStore = new MinStore(tableName, dir, flushThread, compaction, compactThread);
        return minStore;
    }

    @Before
    public void init() throws Exception {
        MinStore minStore = createMinStore();
        Map<String, TableImpl> tables = new HashMap<>();
        tables.put(tableName, new TableImpl(tableName, minStore));
        this.transaction = TransactionManager.newTransaction(tables);
    }

    @Test
    public void test1() throws Exception {
        Table table = transaction.getTable(tableName);

        logger.info("test");
        MinBaseServer minBaseServer = new MinBaseServer();
        final Transaction transaction = minBaseServer.newTransaction();
        try {
            Put put = new Put(key1, column1, value1);
            table.put(put);
            transaction.commit();
        } catch (Exception e) {
            e.printStackTrace();
            transaction.rollback();
        }
    }

//    @Test
//    public void test2() throws Exception{
//        MinBaseServer minBaseServer = new MinBaseServer();
//        final Transaction transaction = minBaseServer.newTransaction();
//        try{
//            transaction.put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("v1"));
//            transaction.put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("v2"));
//            final KeyValue keyValue = transaction.getForUpdate("k1".getBytes());
//            transaction.commit();
//            System.out.println(keyValue);
//        }catch (Exception e){
//            e.printStackTrace();
//            transaction.rollback();
//        }
//
//        final byte[] val = minBaseServer.get("k1".getBytes());
//        System.out.println(new String(val));
//    }
//
//
//    @Test
//    public void test3() throws Exception{
//        MinBaseServer minBaseServer = new MinBaseServer();
//        Thread thread1 = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                final Transaction transaction = minBaseServer.newTransaction();
//                try{
//                    transaction.put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("v1"));
//                    System.out.println("thread1, k1");
//                    try {
//                        Thread.sleep(3000);
//                    } catch (InterruptedException interruptedException) {
//                        interruptedException.printStackTrace();
//                    }
//                    System.out.println("pre thread1, k2");
//                    transaction.put(ByteUtil.toBytes("k2"), ByteUtil.toBytes("v2"));
//                    System.out.println("thread1, k2");
//                    transaction.commit();
//                    System.out.println("commit 1");
//                }catch (Exception e){
//                    e.printStackTrace();
//                    transaction.rollback();
//                }
//            }
//        });
//        Thread thread2 = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException interruptedException) {
//                    interruptedException.printStackTrace();
//                }
//                final Transaction transaction = minBaseServer.newTransaction();
//                try{
//                    System.out.println("pre thread1, k2");
//                    transaction.put(ByteUtil.toBytes("k2"), ByteUtil.toBytes("v3"));
//                    System.out.println("thread2, k2");
//                    try {
//                        Thread.sleep(6000);
//                    } catch (InterruptedException interruptedException) {
//                        interruptedException.printStackTrace();
//                    }
//                    transaction.put(ByteUtil.toBytes("k4"), ByteUtil.toBytes("v4"));
//                    System.out.println("thread2, k4");
//                    transaction.commit();
//                    System.out.println("commit 2");
//                }catch (Exception e){
//                    e.printStackTrace();
//                    transaction.rollback();
//                }
//            }
//        });
//        thread1.start();
//        thread2.start();
//
//        thread1.join();
//        thread2.join();
//
//        final byte[] val = minBaseServer.get("k1".getBytes());
//        System.out.println(new String(val));
//
//        final byte[] val2 = minBaseServer.get("k2".getBytes());
//        System.out.println(new String(val2));
//    }
//
//    @Test
//    public void test4() throws Exception{
//        MinBaseServer minBaseServer = new MinBaseServer();
//        Thread thread1 = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                final Transaction transaction = minBaseServer.newTransaction();
//                try{
//                    transaction.put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("v1"));
//                    System.out.println("thread1, k1");
//                    try {
//                        Thread.sleep(3000);
//                    } catch (InterruptedException interruptedException) {
//                        interruptedException.printStackTrace();
//                    }
//                    System.out.println("pre thread1, k2");
//                    transaction.put(ByteUtil.toBytes("k2"), ByteUtil.toBytes("v2"));
//                    System.out.println("thread1, k2");
//                    transaction.commit();
//                    System.out.println("commit 1");
//                }catch (Exception e){
//                    e.printStackTrace();
//                    transaction.rollback();
//                }
//            }
//        });
//        Thread thread2 = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException interruptedException) {
//                    interruptedException.printStackTrace();
//                }
//                final Transaction transaction = minBaseServer.newTransaction();
//                try{
//                    System.out.println("pre thread1, k2");
//                    transaction.put(ByteUtil.toBytes("k2"), ByteUtil.toBytes("v3"));
//                    System.out.println("thread2, k2");
//                    try {
//                        Thread.sleep(6000);
//                    } catch (InterruptedException interruptedException) {
//                        interruptedException.printStackTrace();
//                    }
//                    transaction.put(ByteUtil.toBytes("k3"), ByteUtil.toBytes("v1"));
//                    System.out.println("thread2, k3");
//                    transaction.commit();
//                    System.out.println("commit 2");
//                }catch (Exception e){
//                    e.printStackTrace();
//                    transaction.rollback();
//                }
//            }
//        });
//
//        Thread thread3 = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException interruptedException) {
//                    interruptedException.printStackTrace();
//                }
//                final Transaction transaction = minBaseServer.newTransaction();
//                try{
//                    System.out.println("pre thread1, k3");
//                    transaction.put(ByteUtil.toBytes("k3"), ByteUtil.toBytes("v3"));
//                    System.out.println("thread2, k3");
//                    try {
//                        Thread.sleep(6000);
//                    } catch (InterruptedException interruptedException) {
//                        interruptedException.printStackTrace();
//                    }
//                    transaction.put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("v1"));
//                    System.out.println("thread2, k1");
//                    transaction.commit();
//                    System.out.println("commit 2");
//                }catch (Exception e){
//                    e.printStackTrace();
//                    transaction.rollback();
//                }
//            }
//        });
//
//        thread1.start();
//        thread2.start();
//        thread3.start();
//
//        thread1.join();
//        thread2.join();
//        thread3.join();
//
////        final byte[] val = minBase.get("k1".getBytes());
////        System.out.println(new String(val));
////
////        final byte[] val2 = minBase.get("k2".getBytes());
////        System.out.println(new String(val2));
//    }
}
