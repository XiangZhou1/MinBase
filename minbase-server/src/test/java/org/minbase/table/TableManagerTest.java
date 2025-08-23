package org.minbase.table;

import org.junit.Test;
import org.minbase.common.table.Table;
import org.minbase.common.table.op.ColumnValues;
import org.minbase.common.table.op.Get;
import org.minbase.common.table.op.Put;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.conf.Configuration;
import org.minbase.server.table.TableManager;
import org.minbase.server.table.Transaction;
import org.minbase.server.table.TransactionManager;

import java.util.Map;

public class TableManagerTest {

    @Test
    public void test1() throws Exception {
        TableManager tableManager = new TableManager(new Configuration());
    }

    @Test
    public void testCreateTable() throws Exception {
        TableManager tableManager = new TableManager(new Configuration());
        tableManager.createTable("table");
    }

    @Test
    public void testCreate() throws Exception {
        TableManager tableManager = new TableManager(new Configuration());
        tableManager.createTable("table");
        Put put = new Put(ByteUtil.toBytes("key1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v1"));
        tableManager.put("table", put);
        tableManager.get("table", new Get(ByteUtil.toBytes("key1")));
    }

    @Test
    public void testCreate2() throws Exception {
        TableManager tableManager = new TableManager(new Configuration());
        tableManager.createTable("table");
        Put put = new Put(ByteUtil.toBytes("key1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v1"));
        tableManager.put("table", put);
        tableManager.get("table", new Get(ByteUtil.toBytes("key1")));

        Put put2 = new Put(ByteUtil.toBytes("key1"), ByteUtil.toBytes("c2"), ByteUtil.toBytes("v2"));
        tableManager.put("table", put2);
        ColumnValues columnValues2 = tableManager.get("table", new Get(ByteUtil.toBytes("key1")));
        System.out.println(columnValues2);

        tableManager.foreFlush();
    }

    @Test
    public void testLoadAndGet() throws Exception {
        TableManager tableManager = new TableManager(new Configuration());
        ColumnValues columnValues = tableManager.get("table", new Get(ByteUtil.toBytes("key1")));
        assert columnValues.getColumnValues().size() == 2;
        for (Map.Entry<byte[], byte[]> entry : columnValues.getColumnValues().entrySet()) {
            System.out.println(new String(entry.getKey()) + ":" + new String(entry.getValue()));
        }
    }

    @Test
    public void tesTxPutAndGet() throws Exception {
        TableManager tableManager = new TableManager(new Configuration());
        TransactionManager transactionManager = tableManager.getTransactionManager();

        // 先txPut
        Transaction transaction = transactionManager.newTransaction();
        Table table1 = transaction.getTable("table");
        table1.put(new Put(ByteUtil.toBytes("key1"), ByteUtil.toBytes("c3"), ByteUtil.toBytes("v4")));

        ColumnValues columnValues = tableManager.get("table", new Get(ByteUtil.toBytes("key1")));

        for (Map.Entry<byte[], byte[]> entry : columnValues.getColumnValues().entrySet()) {
            System.out.println(new String(entry.getKey()) + ":" + new String(entry.getValue()));
        }

        transaction.commit();

        ColumnValues columnValues3 = tableManager.get("table", new Get(ByteUtil.toBytes("key1")));
        for (Map.Entry<byte[], byte[]> entry : columnValues3.getColumnValues().entrySet()) {
            System.out.println(new String(entry.getKey()) + ":" + new String(entry.getValue()));
        }
    }


    @Test
    public void tesTxPutAnForceFlush() throws Exception {
        TableManager tableManager = new TableManager(new Configuration());
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    tableManager.foreFlush();
                }
            }
        });
        thread.start();

        for (int i = 0; i < 10000; i++) {
            tableManager.put("table", new Put(ByteUtil.toBytes("key1"), ByteUtil.toBytes("c" + i), ByteUtil.toBytes("v" + i)));
            Thread.sleep(1);
        }
    }

    @Test
    public void tesGet() throws Exception {
        TableManager tableManager = new TableManager(new Configuration());
        ColumnValues columnValues = tableManager.get("table", new Get(ByteUtil.toBytes("key1")));
        for (Map.Entry<byte[], byte[]> entry : columnValues.getColumnValues().entrySet()) {
            System.out.println(new String(entry.getKey()) + ":" + new String(entry.getValue()));
        }
        assert columnValues.size() == 10000;
    }


    @Test
    public void tesTxPutAnForceFlushAndGet() throws Exception {
        TableManager tableManager = new TableManager(new Configuration());
        tableManager.createTable("table");
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(500);
                    } catch (Exception e) {
                        System.out.println(e);
                    }

                    tableManager.foreFlush();
                }
            }
        });
        thread.start();

        Thread getThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(200);
                    } catch (Exception e) {
                        System.out.println(e);
                    }

                    ColumnValues columnValues = tableManager.get("table", new Get(ByteUtil.toBytes("key1")));
                    int size = columnValues.size();
                    for (int i = 0; i < size; i++) {
                        byte[] bytes = columnValues.get(ByteUtil.toBytes("c" + i));
                        assert bytes != null;
                        assert ByteUtil.byteEqual(bytes, ByteUtil.toBytes("v" + i));
                    }
                }
            }
        });
        getThread.start();

        for (long i = 0; i < 100000000000L; i++) {
            tableManager.put("table", new Put(ByteUtil.toBytes("key1"), ByteUtil.toBytes("c" + i), ByteUtil.toBytes("v" + i)));
            Thread.sleep(1);
        }
    }
}
