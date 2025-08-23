package org.minbase.table;

import org.junit.Test;
import org.minbase.common.exception.TransactionException;
import org.minbase.common.table.Table;
import org.minbase.common.table.op.ColumnValues;
import org.minbase.common.table.op.Get;
import org.minbase.common.table.op.Put;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.conf.Configuration;
import org.minbase.server.table.TableManager;
import org.minbase.server.table.Transaction;
import org.minbase.server.table.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransactionTest {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionTest.class);
    public static TableManager tableManager ;
    static {
        try {
            tableManager = new TableManager(new Configuration());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    @Test
    public void testCreateTx() {
        TransactionManager transactionManager = new TransactionManager(tableManager);
        Transaction transaction = transactionManager.newTransaction();
    }

    @Test
    public void testOverLay() throws IOException {
        TransactionManager transactionManager = tableManager.getTransactionManager();
        tableManager.createTable("table1");
        tableManager.createTable("table2");
        Transaction transaction = transactionManager.newTransaction();
        Table table1 = transaction.getTable("table1");
        table1.put(new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v1")));
        transaction.commit();

        Transaction transaction2 = transactionManager.newTransaction();
        Table table11 = transaction2.getTable("table1");
        byte[] c1 = ByteUtil.toBytes("c1");
        List<byte[]> columns = new ArrayList<>();
        columns.add(c1);
        table11.get(new Get(ByteUtil.toBytes("k1"), columns));

        Transaction transaction3 = transactionManager.newTransaction();
        Table table13 = transaction3.getTable("table1");
        byte[] c2 = ByteUtil.toBytes("c2");
        List<byte[]> columns2 = new ArrayList<>();
        columns2.add(c2);
        table13.get(new Get(ByteUtil.toBytes("k2"), columns2));

        assert !transaction.getWriteSet().isOverLap(transaction3.getReadSet());

        Transaction transaction4 = transactionManager.newTransaction();
        Table table14 = transaction4.getTable("table2");
        byte[] c21 = ByteUtil.toBytes("c1");
        List<byte[]> columns21 = new ArrayList<>();
        columns21.add(c21);
        table14.get(new Get(ByteUtil.toBytes("k1"), columns21));

        assert !transaction.getWriteSet().isOverLap(transaction4.getReadSet());
    }

    @Test
    public void testTxput() throws IOException {
        TransactionManager transactionManager = tableManager.getTransactionManager();
        tableManager.createTable("table1");
        Transaction transaction = transactionManager.newTransaction();
        Table table1 = transaction.getTable("table1");
        table1.put(new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v1")));

        Transaction transaction2= transactionManager.newTransaction();
        Table table12 = transaction.getTable("table1");
        table12.put(new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v1")));

        transaction.commit();
        transaction2.commit();
        System.out.println(transactionManager);
    }

    @Test
    public void testTxputAndGet1() throws IOException {
        TransactionManager transactionManager = tableManager.getTransactionManager();
        tableManager.createTable("table1");
        Transaction transaction = transactionManager.newTransaction();
        Table table1 = transaction.getTable("table1");
        table1.put(new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v1")));

        Transaction transaction2 = transactionManager.newTransaction();
        Table table11 = transaction2.getTable("table1");
        byte[] c1 = ByteUtil.toBytes("c1");
        List<byte[]> columns = new ArrayList<>();
        columns.add(c1);
        table11.get(new Get(ByteUtil.toBytes("k1"), columns));

        transaction.commit();
        transaction2.commit();
        System.out.println(transactionManager);
    }

    @Test
    public void testTxputAndGet2() throws IOException {
        TransactionManager transactionManager = tableManager.getTransactionManager();
        tableManager.createTable("table1");
        Transaction transaction = transactionManager.newTransaction();
        Table table1 = transaction.getTable("table1");
        table1.put(new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v1")));

        Transaction transaction2 = transactionManager.newTransaction();
        Table table11 = transaction2.getTable("table1");
        byte[] c1 = ByteUtil.toBytes("c1");
        List<byte[]> columns = new ArrayList<>();
        columns.add(c1);
        table11.get(new Get(ByteUtil.toBytes("k1"), columns));

        transaction2.commit();
        transaction.commit();
        System.out.println(transactionManager);
    }

    @Test
    public void testTxputAndGet3() throws IOException {
        TransactionManager transactionManager = tableManager.getTransactionManager();
        tableManager.createTable("table1");

        Transaction transaction2 = transactionManager.newTransaction();
        Table table11 = transaction2.getTable("table1");
        byte[] c1 = ByteUtil.toBytes("c1");
        List<byte[]> columns = new ArrayList<>();
        columns.add(c1);
        table11.get(new Get(ByteUtil.toBytes("k1"), columns));

        Transaction transaction = transactionManager.newTransaction();
        Table table1 = transaction.getTable("table1");
        table1.put(new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v1")));

        transaction2.commit();
        transaction.commit();
        System.out.println(transactionManager);
    }

    @Test
    public void testTxputAndGet4() throws IOException {
        TransactionManager transactionManager = tableManager.getTransactionManager();
        tableManager.createTable("table1");

        Transaction transaction2 = transactionManager.newTransaction();
        Table table11 = transaction2.getTable("table1");
        byte[] c1 = ByteUtil.toBytes("c1");
        List<byte[]> columns = new ArrayList<>();
        columns.add(c1);
        table11.get(new Get(ByteUtil.toBytes("k1"), columns));

        Transaction transaction = transactionManager.newTransaction();
        Table table1 = transaction.getTable("table1");
        table1.put(new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v1")));

        boolean commitFail = false;
        transaction.commit();
        try {
            transaction2.commit();
        } catch (TransactionException e) {
            commitFail = true;
        }
        if (!commitFail) {
            throw new RuntimeException("Error");
        }
    }

    @Test
    public void testTxputConcurrent() throws Exception {
        TransactionManager transactionManager = tableManager.getTransactionManager();
        tableManager.createTable("table1");
        tableManager.createTable("table2");
        for (int i = 0; i < 1000000; i++) {
            Thread thread1 = new Thread(new Runnable() {
                @Override
                public void run() {
                    Transaction transaction = transactionManager.newTransaction();
                    Table table1 = transaction.getTable("table1");
                    table1.put(new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v1")));
                    Table table2 = transaction.getTable("table2");
                    table2.put(new Put(ByteUtil.toBytes("k2"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v1")));
                    transaction.commit();
                }
            });
            Thread thread2 = new Thread(new Runnable() {
                @Override
                public void run() {
                    Transaction transaction = transactionManager.newTransaction();
                    Table table1 = transaction.getTable("table1");
                    table1.put(new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v2")));
                    Table table2 = transaction.getTable("table2");
                    table2.put(new Put(ByteUtil.toBytes("k2"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v2")));
                    transaction.commit();
                }
            });
            thread1.start();
            thread2.start();
            thread1.join();
            thread2.join();

            ColumnValues columnValues = tableManager.get("table1", new Get(ByteUtil.toBytes("k1")));
            ColumnValues columnValues2 = tableManager.get("table2", new Get(ByteUtil.toBytes("k2")));
            assert ByteUtil.byteEqual(columnValues.get(ByteUtil.toBytes("c1")), columnValues2.get(ByteUtil.toBytes("c1")));
        }
    }

    @Test
    public void testTxputConcurrent2() throws Exception {
        TransactionManager transactionManager = tableManager.getTransactionManager();
        tableManager.createTable("table1");
        tableManager.createTable("table2");
        for (int i = 0; i < 1000000; i++) {
            Thread thread1 = new Thread(new Runnable() {
                @Override
                public void run() {
                    Transaction transaction = transactionManager.newTransaction();
                    Table table1 = transaction.getTable("table1");
                    table1.put(new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v1")));
                    Table table2 = transaction.getTable("table2");
                    table2.put(new Put(ByteUtil.toBytes("k2"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v1")));
                    transaction.commit();
                }
            });
            Thread thread2 = new Thread(new Runnable() {
                @Override
                public void run() {
                    Transaction transaction = transactionManager.newTransaction();
                    Table table1 = transaction.getTable("table1");
                    table1.put(new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v2")));
                    Table table2 = transaction.getTable("table2");
                    table2.put(new Put(ByteUtil.toBytes("k2"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v2")));
                    transaction.commit();
                }
            });

            Thread thread3 = new Thread(new Runnable() {
                @Override
                public void run() {
                    Transaction transaction = transactionManager.newTransaction();
                    Table table1 = transaction.getTable("table1");
                    ColumnValues columnValues = table1.get(new Get(ByteUtil.toBytes("k1")));
                    Table table2 = transaction.getTable("table2");
                    ColumnValues columnValues2 = table2.get(new Get(ByteUtil.toBytes("k2")));
                    assert ByteUtil.byteEqual(columnValues.get(ByteUtil.toBytes("c1")), columnValues2.get(ByteUtil.toBytes("c1")));
                    transaction.rollback();
                }
            });
            thread1.start();
            thread2.start();
            thread3.start();
            thread1.join();
            thread2.join();
            thread3.join();

            ColumnValues columnValues = tableManager.get("table1", new Get(ByteUtil.toBytes("k1")));
            ColumnValues columnValues2 = tableManager.get("table2", new Get(ByteUtil.toBytes("k2")));
            assert ByteUtil.byteEqual(columnValues.get(ByteUtil.toBytes("c1")), columnValues2.get(ByteUtil.toBytes("c1")));
        }
    }

    @Test
    public void testTxPutAndGet() throws Exception {
        TransactionManager transactionManager = tableManager.getTransactionManager();
        tableManager.createTable("table1");
        tableManager.createTable("table2");

        // 测试能不能get到commit的kv
        tableManager.put("table1", new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v2")));
        ColumnValues columnValues = tableManager.get("table1", new Get(ByteUtil.toBytes("k1")));
        assert ByteUtil.byteEqual(columnValues.get(ByteUtil.toBytes("c1")), ByteUtil.toBytes("v2"));

        // 测试能不能get 到localStore的东西
        Transaction transaction = transactionManager.newTransaction();
        Table table1 = transaction.getTable("table1");
        table1.put(new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v3")));
        ColumnValues columnValues2 = table1.get(new Get(ByteUtil.toBytes("k1")));
        assert ByteUtil.byteEqual(columnValues2.get(ByteUtil.toBytes("c1")), ByteUtil.toBytes("v3"));
        transaction.rollback();

        // 测试rollback之后, 能不能get到commit的kv
        ColumnValues columnValues3 = tableManager.get("table1", new Get(ByteUtil.toBytes("k1")));
        assert ByteUtil.byteEqual(columnValues3.get(ByteUtil.toBytes("c1")), ByteUtil.toBytes("v2"));


        // 在transaction过程不能get到在过程中已经committed的内容
        Transaction transaction2 = transactionManager.newTransaction();
        Table table11 = transaction2.getTable("table1");
        table11.put(new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v4")));
        tableManager.put("table1", new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v5")));
        ColumnValues columnValues4 = table11.get(new Get(ByteUtil.toBytes("k1")));
        assert ByteUtil.byteEqual(columnValues4.get(ByteUtil.toBytes("c1")), ByteUtil.toBytes("v4"));

        // 测试能get到后commit的内容
        boolean commitFail = false;
        try{
            transaction2.commit();
        } catch (TransactionException e) {
            commitFail = true;
            transaction2.rollback();
        }

        if (!commitFail) {
            throw new RuntimeException("Commit should fail");
        }

        ColumnValues columnValues5 = tableManager.get("table1", new Get(ByteUtil.toBytes("k1")));
        assert ByteUtil.byteEqual(columnValues5.get(ByteUtil.toBytes("c1")), ByteUtil.toBytes("v5"));
    }

    @Test
    public void testTxputConcurrent3() throws Exception {
        TransactionManager transactionManager = tableManager.getTransactionManager();

        tableManager.createTable("table1");
        tableManager.createTable("table2");
        tableManager.put("table1", new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v1")));
        tableManager.put("table2", new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v2")));

        Transaction transaction = transactionManager.newTransaction();
        Transaction transaction2 = transactionManager.newTransaction();
        try {
            Table table2 = transaction2.getTable("table2");
            ArrayList<byte[]> columns = new ArrayList<>();
            columns.add(ByteUtil.toBytes("c1"));
            ColumnValues columnValues = table2.get(new Get(ByteUtil.toBytes("k1"), columns));
            if (ByteUtil.byteEqual(columnValues.get(ByteUtil.toBytes("c1")), ByteUtil.toBytes("v2"))) {
                Table table1 = transaction2.getTable("table1");
                table1.put(new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v2")));
            }
            transaction2.commit();
        } catch (Exception e) {
            System.out.println("transaction 2 commit fail");
            transaction2.rollback();
        }

        try {
            Table table1 = transaction.getTable("table1");
            ArrayList<byte[]> columns = new ArrayList<>();
            columns.add(ByteUtil.toBytes("c1"));
            ColumnValues columnValues = table1.get(new Get(ByteUtil.toBytes("k1"), columns));
            if (ByteUtil.byteEqual(columnValues.get(ByteUtil.toBytes("c1")), ByteUtil.toBytes("v1"))) {
                Table table2 = transaction.getTable("table2");
                table2.put(new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v1")));
            }
            transaction.commit();
        } catch (Exception e) {
            System.out.println("transaction 1 commit fail");
            transaction.rollback();
        }

        ColumnValues columnValues = tableManager.get("table1", new Get(ByteUtil.toBytes("k1")));
        ColumnValues columnValues2 = tableManager.get("table2", new Get(ByteUtil.toBytes("k1")));
        assert ByteUtil.byteEqual(columnValues.get(ByteUtil.toBytes("c1")), columnValues2.get(ByteUtil.toBytes("c1")));
    }

    @Test
    public void testTxputConcurrent4() throws Exception {
        TransactionManager transactionManager = tableManager.getTransactionManager();

        tableManager.createTable("table1");
        tableManager.createTable("table2");
        tableManager.put("table1", new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v1")));
        tableManager.put("table2", new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v2")));

        Transaction transaction = transactionManager.newTransaction();
        Transaction transaction2 = transactionManager.newTransaction();
        try {
            Table table2 = transaction2.getTable("table2");
            ArrayList<byte[]> columns = new ArrayList<>();
            columns.add(ByteUtil.toBytes("c1"));
            ColumnValues columnValues = table2.get(new Get(ByteUtil.toBytes("k1"), columns));
            if (ByteUtil.byteEqual(columnValues.get(ByteUtil.toBytes("c1")), ByteUtil.toBytes("v2"))) {
                Table table1 = transaction2.getTable("table1");
                table1.put(new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v2")));
            }

        } catch (Exception e) {
            System.out.println("transaction 2 write fail");
            //transaction2.rollback();
        }

        try {
            Table table1 = transaction.getTable("table1");
            ArrayList<byte[]> columns = new ArrayList<>();
            columns.add(ByteUtil.toBytes("c1"));
            ColumnValues columnValues = table1.get(new Get(ByteUtil.toBytes("k1"), columns));
            if (ByteUtil.byteEqual(columnValues.get(ByteUtil.toBytes("c1")), ByteUtil.toBytes("v1"))) {
                Table table2 = transaction.getTable("table2");
                table2.put(new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v1")));
            }
            transaction.commit();
        } catch (Exception e) {
            System.out.println("transaction 1 commit fail");
            transaction.rollback();
        }

        try {
            transaction2.commit();
        } catch (Exception e) {
            System.out.println("transaction 2 commit fail");
            transaction2.rollback();
        }

        ColumnValues columnValues = tableManager.get("table1", new Get(ByteUtil.toBytes("k1")));
        ColumnValues columnValues2 = tableManager.get("table2", new Get(ByteUtil.toBytes("k1")));
        assert ByteUtil.byteEqual(columnValues.get(ByteUtil.toBytes("c1")), columnValues2.get(ByteUtil.toBytes("c1")));
    }
    static Transaction transaction2 = null;
    static Transaction transaction1 = null;
    @Test
    public void testTxputConcurrent5() throws Exception {
        TransactionManager transactionManager = tableManager.getTransactionManager();

        tableManager.createTable("table1");
        tableManager.createTable("table2");
        for(int i=0; i<1000000; i++) {
            tableManager.put("table1", new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v1")));
            tableManager.put("table2", new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v2")));


            Thread thread1 = new Thread(new Runnable() {
                @Override
                public void run() {
                    transaction1 = transactionManager.newTransaction();
                    try {
                        Table table1 = transaction1.getTable("table1");
                        ArrayList<byte[]> columns = new ArrayList<>();
                        columns.add(ByteUtil.toBytes("c1"));
                        ColumnValues columnValues = table1.get(new Get(ByteUtil.toBytes("k1"), columns));
                        if (ByteUtil.byteEqual(columnValues.get(ByteUtil.toBytes("c1")), ByteUtil.toBytes("v1"))) {
                            Table table2 = transaction1.getTable("table2");
                            table2.put(new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v1")));
                        }
                        transaction1.commit();
                    } catch (Exception e) {
                        System.out.println("transaction 1 commit fail");
                        transaction1.rollback();
                    }
                }
            });

            Thread thread2 = new Thread(new Runnable() {
                @Override
                public void run() {
                    transaction2 = transactionManager.newTransaction();
                    try {
                        Table table2 = transaction2.getTable("table2");
                        ArrayList<byte[]> columns = new ArrayList<>();
                        columns.add(ByteUtil.toBytes("c1"));
                        ColumnValues columnValues = table2.get(new Get(ByteUtil.toBytes("k1"), columns));
                        if (ByteUtil.byteEqual(columnValues.get(ByteUtil.toBytes("c1")), ByteUtil.toBytes("v2"))) {
                            Table table1 = transaction2.getTable("table1");
                            table1.put(new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v2")));
                        }
                        transaction2.commit();
                    } catch (Exception e) {
                        System.out.println("transaction 2 write fail");
                        transaction2.rollback();
                    }
                }
            });
            thread1.start();
            thread2.start();
            thread1.join();
            thread2.join();

            ColumnValues columnValues = tableManager.get("table1", new Get(ByteUtil.toBytes("k1")));
            ColumnValues columnValues2 = tableManager.get("table2", new Get(ByteUtil.toBytes("k1")));
            boolean byteEqual = ByteUtil.byteEqual(columnValues.get(ByteUtil.toBytes("c1")), columnValues2.get(ByteUtil.toBytes("c1")));
            if (!byteEqual) {
                LOG.info("Fail loop {}, transaction1 {}", i, transaction1);
                LOG.info("Fail loop {}, transaction2 {}", i, transaction2);
                assert false;
            } else {
                LOG.info("Success loop {}, transaction1 {}", i, transaction1);
                LOG.info("Success loop {}, transaction2 {}", i, transaction2);
            }
        }
    }

    @Test
    public void testTxputConcurrent6() throws Exception {
        TransactionManager transactionManager = tableManager.getTransactionManager();

        tableManager.createTable("table1");
        tableManager.createTable("table2");
        for(int i=0; i<1000000; i++) {
            tableManager.put("table1", new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v1")));
            tableManager.put("table2", new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v2")));


            Thread thread1 = new Thread(new Runnable() {
                @Override
                public void run() {
                    transaction1 = transactionManager.newTransaction();
                    try {
                        Table table1 = transaction1.getTable("table1");
                        ArrayList<byte[]> columns = new ArrayList<>();
                        columns.add(ByteUtil.toBytes("c1"));
                        ColumnValues columnValues = table1.get(new Get(ByteUtil.toBytes("k1"), columns));
                        if (ByteUtil.byteEqual(columnValues.get(ByteUtil.toBytes("c1")), ByteUtil.toBytes("v1"))) {
                            Table table2 = transaction1.getTable("table2");
                            table2.put(new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v1")));
                        }
                        transaction1.commit();
                    } catch (Exception e) {
                        System.out.println("transaction 1 commit fail");
                        transaction1.rollback();
                    }
                    tableManager.foreFlush();
                }
            });

            Thread thread2 = new Thread(new Runnable() {
                @Override
                public void run() {
                    transaction2 = transactionManager.newTransaction();
                    try {
                        Table table2 = transaction2.getTable("table2");
                        ArrayList<byte[]> columns = new ArrayList<>();
                        columns.add(ByteUtil.toBytes("c1"));
                        ColumnValues columnValues = table2.get(new Get(ByteUtil.toBytes("k1"), columns));
                        if (ByteUtil.byteEqual(columnValues.get(ByteUtil.toBytes("c1")), ByteUtil.toBytes("v2"))) {
                            Table table1 = transaction2.getTable("table1");
                            table1.put(new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v2")));
                        }
                        transaction2.commit();
                    } catch (Exception e) {
                        System.out.println("transaction 2 write fail");
                        transaction2.rollback();
                    }
                    tableManager.foreFlush();
                }
            });
            thread1.start();
            thread2.start();
            thread1.join();
            thread2.join();

            ColumnValues columnValues = tableManager.get("table1", new Get(ByteUtil.toBytes("k1")));
            ColumnValues columnValues2 = tableManager.get("table2", new Get(ByteUtil.toBytes("k1")));
            boolean byteEqual = ByteUtil.byteEqual(columnValues.get(ByteUtil.toBytes("c1")), columnValues2.get(ByteUtil.toBytes("c1")));
            if (!byteEqual) {
                LOG.info("Fail loop {}, transaction1 {}", i, transaction1);
                LOG.info("Fail loop {}, transaction2 {}", i, transaction2);
                assert false;
            } else {
                LOG.info("Success loop {}, transaction1 {}", i, transaction1);
                LOG.info("Success loop {}, transaction2 {}", i, transaction2);
            }
        }
    }
}
