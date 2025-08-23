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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ReadPointTest {
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
    public void testTxputConcurrent() throws Exception {
        tableManager.createTable("table1");
        ConcurrentSkipListMap<Long, byte[]> valuesMap = new ConcurrentSkipListMap<>();
        ExecutorService executorService = Executors.newFixedThreadPool(1000000);
        TransactionManager transactionManager = tableManager.getTransactionManager();
        txPut(transactionManager, valuesMap, 0);

        for (int i = 1; i < 1000000; i++) {
            final int valueIndex = i;
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    txPut(transactionManager, valuesMap, valueIndex);
                    tableManager.foreFlush();
                }
            });

            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    txGet(transactionManager, valuesMap);
                }
            });
            Thread.sleep(10);
        }
        executorService.shutdown();
        executorService.wait();
    }

    private static void txGet(TransactionManager transactionManager, ConcurrentSkipListMap<Long, byte[]> valuesMap) {
        Transaction transaction2 = transactionManager.newTransaction();
        try {
            //Thread.sleep(100 * new Random().nextInt(100));
            Table table2 = transaction2.getTable("table1");
            ArrayList<byte[]> columns = new ArrayList<>();
            columns.add(ByteUtil.toBytes("c1"));
            ColumnValues columnValues = table2.get(new Get(ByteUtil.toBytes("k1"), columns));
            byte[] getResult = columnValues.get(ByteUtil.toBytes("c1"));
            long readPoint = transaction2.getReadPoint();
            Long l = valuesMap.floorKey(readPoint);
            byte[] targetResult = valuesMap.get(l);
            System.out.println(String.format("get readPoint %d, getResult %s, targetResult %s, tableManager.readPoint %d)", readPoint, new String(getResult), new String(targetResult), tableManager.getReadPoint()));
            assert ByteUtil.byteEqual(getResult, targetResult);
            transaction2.rollback();
        } catch (Exception e) {
            System.out.println("transaction 2 get fail");
            e.printStackTrace();
            transaction2.rollback();
        }
    }

    private static void txPut(TransactionManager transactionManager, ConcurrentSkipListMap<Long, byte[]> valuesMap, int valueIndex) {
        Transaction transaction1 = transactionManager.newTransaction();
        try {
            Table table1 = transaction1.getTable("table1");
            table1.put(new Put(ByteUtil.toBytes("k1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v" + valueIndex)));
            transaction1.commit();
            valuesMap.put(transaction1.getCommitId(), ByteUtil.toBytes("v" + valueIndex));
        } catch (Exception e) {
            System.out.println("transaction 1 commit fail");
            transaction1.rollback();
        }
    }
}
