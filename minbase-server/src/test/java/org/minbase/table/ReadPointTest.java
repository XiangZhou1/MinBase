package org.minbase.table;

import org.junit.Test;
import org.minbase.common.table.Table;
import org.minbase.common.table.op.ColumnValues;
import org.minbase.common.table.op.Get;
import org.minbase.common.table.op.Put;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.conf.Configuration;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.Op;
import org.minbase.server.kv.Value;
import org.minbase.server.kv.compaction.CompactionChecker;
import org.minbase.server.kv.compaction.CompactionResult;
import org.minbase.server.kv.compaction.MinorCompactionPolicy;
import org.minbase.server.kv.storage.StoreFile;
import org.minbase.server.kv.storage.StoreFileBuilder;
import org.minbase.server.kv.storage.StoreFileIterator;
import org.minbase.server.kv.storage.StoreFileManager;
import org.minbase.server.kv.store.StoreManager;
import org.minbase.server.table.TableManager;
import org.minbase.server.table.Transaction;
import org.minbase.server.table.TransactionManager;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
        ExecutorService executorService = Executors.newFixedThreadPool(100);
        TransactionManager transactionManager = tableManager.getTransactionManager();
        txPut(transactionManager, valuesMap, 0);

        executorService.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 1; i < 100000; i++) {
                    txPut(transactionManager, valuesMap, i);
                    if (i % 100 == 0) {
                        tableManager.foreFlush();
                    }
                    try {
                        Thread.sleep(2);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });

        for (int i = 1; i < 90; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        txGet(transactionManager, valuesMap);
                    }
                }
            });
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    private static void txGet(TransactionManager transactionManager, ConcurrentSkipListMap<Long, byte[]> valuesMap) {
        Transaction transaction2 = transactionManager.newTransaction();
        try {
            Thread.sleep(300 * new Random().nextInt(100));
            Table table2 = transaction2.getTable("table1");
            ArrayList<byte[]> columns = new ArrayList<>();
            columns.add(ByteUtil.toBytes("c1"));
            ColumnValues columnValues = table2.get(new Get(ByteUtil.toBytes("k1"), columns));
            byte[] getResult = columnValues.get(ByteUtil.toBytes("c1"));
            if (getResult == null) {
                getResult = new byte[0];
            }
            long readPoint = transaction2.getReadPoint();
            Long l = valuesMap.floorKey(readPoint);
            byte[] targetResult = valuesMap.get(l);
            System.out.println(String.format("get readPoint %d, getResult %s, targetResult %s, tableManager.readPoint %d)", readPoint, new String(getResult), new String(targetResult), tableManager.getReadPoint()));
            if (!ByteUtil.byteEqual(getResult, targetResult)) {
                assert false;
                System.exit(-1);
            }
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

    public static StoreFileManager storeFileManager;
    public static StoreManager storeManager;

    static {
        storeFileManager = Mockito.mock(StoreFileManager.class);
        storeManager = Mockito.mock(StoreManager.class);
        try {
            Mockito.when(storeFileManager.newTmpStroeFile()).thenReturn(new StoreFileBuilder(1000));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testGet() throws Exception {

        StoreFileBuilder storeFileBuilder = new StoreFileBuilder(10000);
        storeFileBuilder.add(new KeyValue(new Key(ByteUtil.toBytes("k0"), 1), new Value(Op.PUT, ByteUtil.toBytes("v1"))));
        storeFileBuilder.add(new KeyValue(new Key(ByteUtil.toBytes("k1"), 2), new Value(Op.PUT, ByteUtil.toBytes("v2"))));
        StoreFile storeFile1 = storeFileBuilder.build();
        saveStoreFile(storeFile1);

        StoreFileBuilder storeFileBuilder2 = new StoreFileBuilder(10000);
        storeFileBuilder2.add(new KeyValue(new Key(ByteUtil.toBytes("k1"), 3), new Value(Op.PUT, ByteUtil.toBytes("v3"))));
        StoreFile storeFile2 = storeFileBuilder2.build();
        saveStoreFile(storeFile2);

        StoreFileBuilder storeFileBuilder3 = new StoreFileBuilder(10000);
        storeFileBuilder3.add(new KeyValue(new Key(ByteUtil.toBytes("k1"), 4), new Value(Op.PUT, ByteUtil.toBytes("v4"))));
        StoreFile storeFile3 = storeFileBuilder3.build();
        saveStoreFile(storeFile3);

        StoreFileBuilder storeFileBuilder4 = new StoreFileBuilder(10000);
        storeFileBuilder4.add(new KeyValue(new Key(ByteUtil.toBytes("k1"), 5), new Value(Op.PUT, ByteUtil.toBytes("v5"))));
        StoreFile storeFile4 = storeFileBuilder4.build();
        saveStoreFile(storeFile4);

        Mockito.when(storeFileManager.getCompactionPolicy()).thenReturn(new MinorCompactionPolicy(new Configuration()));
        Mockito.when(storeManager.getMinReadPoint()).thenReturn(3L);

        List<StoreFile> storeFiles = new ArrayList<>();
        storeFiles.add(storeFile1);
        storeFiles.add(storeFile2);
        storeFiles.add(storeFile3);
        storeFiles.add(storeFile4);
        Mockito.when(storeFileManager.getStoreFilesToCompact()).thenReturn(storeFiles);
        CompactionChecker compactionChecker = new CompactionChecker(storeManager, storeFileManager);
        CompactionResult compact = compactionChecker.compact();

        for (StoreFile storeFile : compact.getFilesToAdd()) {
            saveStoreFile(storeFile);
        }
    }

    public void saveStoreFile(StoreFile storeFile) throws IOException {
        File dir = new File("data/stor");
        if (!dir.exists()) {
            dir.mkdirs();
        }
        File file = new File("data/stor", storeFile.getStoreId() + ".tmp");
        storeFile.setRawFile(file);
        try (FileOutputStream outputStream = new FileOutputStream(storeFile.getRawFile())) {
            storeFile.encodeToStream(outputStream);
            outputStream.flush();
            outputStream.getChannel().force(true);
        }
        File file2 = new File("data/stor", storeFile.getStoreId());
        file.renameTo(file2);
        storeFile.setRawFile(file2);
    }
}
