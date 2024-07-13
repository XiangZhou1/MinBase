package org.minbase.server.storage.compaction;

import org.junit.Before;
import org.junit.Test;
import org.minbase.server.iterator.KeyIterator;
import org.minbase.server.iterator.MergeIterator;
import org.minbase.server.iterator.SSTableIterator;
import org.minbase.server.lsmStorage.LevelStorageManager;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.op.Value;
import org.minbase.server.storage.sstable.SSTBuilder;
import org.minbase.server.storage.sstable.SSTable;
import org.minbase.common.utils.ByteUtils;
import org.minbase.common.utils.Utils;

import java.io.File;
import java.util.Arrays;
import java.util.Random;

public class LevelCompactionTest {

    LevelStorageManager levelStorageManager;

    @Before
    public void before() throws Exception{
        final File file = new File("D:\\Project\\MinBase\\minbase-server\\data");
//        if(!file.delete()){
//            throw new RuntimeException("file not delete");
//        }
        levelStorageManager = new LevelStorageManager();
        //initDataFile();
    }


    // 将compaction关闭
    @Test
    public void initDataFile() throws Exception {
        int num = 10;
        Random random = new Random();
        SSTBuilder sstBuilder = new SSTBuilder();
        int sequence = 0;
        for(int k = 0; k < 100; k++){
            final int no = random.nextInt(10);
            for (int i = 0; i < num; i++) {
                sstBuilder.add(new KeyValue(new Key(ByteUtils.toBytes(("k" + Utils.fillZero(no+i))), sequence), Value.Put(ByteUtils.toBytes("v" + Utils.fillZero(sequence)))));
                sequence ++;
            }
            final SSTable ssTable = sstBuilder.build();
            levelStorageManager.addNewSSTable(ssTable, sequence);
            sstBuilder = new SSTBuilder();
        }
    }

    @Test
    public void testSSTableIter() throws Exception {
        final SSTable ssTable = levelStorageManager.loadSSTable("0e15dd05-f472-4e49-8a64-dcd681fa71e9");
        final SSTableIterator iterator = ssTable.iterator();
        while (iterator.isValid()){
            System.out.println(iterator.value());
            iterator.nextUserKey();
        }


        final SSTableIterator iterator1 = ssTable.iterator();
        while (iterator1.isValid()){
            System.out.println(iterator1.value());
            iterator1.nextKey();
        }

        System.out.println("sstable2");
        final SSTable ssTable2 = levelStorageManager.loadSSTable("0e65787e-6980-444f-b643-d16607fb567a");
        final SSTableIterator iterator2 = ssTable2.iterator();
        while (iterator2.isValid()){
            System.out.println(iterator2.value());
            iterator2.nextUserKey();
        }

        final SSTableIterator iterator3 = ssTable2.iterator();
        while (iterator3.isValid()){
            System.out.println(iterator3.value());
            iterator3.nextKey();
        }

        System.out.println("mergeIterator");
        MergeIterator mergeIterator = new MergeIterator(Arrays.asList(ssTable.iterator(), ssTable2.iterator()));
        while (mergeIterator.isValid()){
            System.out.println(mergeIterator.value());
            mergeIterator.nextUserKey();
        }
    }


    @Test
    public void test1() throws Exception {
        levelStorageManager.loadSSTables();

        final KeyIterator iterator = levelStorageManager.iterator(null, null);
        while (iterator.isValid()) {
            final KeyValue keyValue = iterator.value();
            System.out.println(keyValue);
            //assert new String(keyValue.getKey().getUserKey()).substring(1).equals(new String(keyValue.getValue().value()).substring(1));
            iterator.nextKey();
        }

        final KeyIterator iterator2 = levelStorageManager.iterator(null, null);
        while (iterator2.isValid()) {
            final KeyValue keyValue = iterator2.value();
            System.out.println(keyValue);
            //assert new String(keyValue.getKey().getUserKey()).substring(1).equals(new String(keyValue.getValue().value()).substring(1));
            iterator2.nextUserKey();
        }

    }

    @Test
    public void test2() throws Exception {
        LevelCompaction levelCompaction = new LevelCompaction(levelStorageManager);
        levelStorageManager.loadSSTables();

        while (levelCompaction.shouldCompact()) {
            levelCompaction.compact();
        }

        System.out.println("scan");
        final KeyIterator iterator2 = levelStorageManager.iterator(null, null);
        while (iterator2.isValid()) {
            final KeyValue keyValue = iterator2.value();
            System.out.println(keyValue);
            //assert new String(keyValue.getKey().getUserKey()).substring(1).equals(new String(keyValue.getValue().value()).substring(1));
            iterator2.nextKey();
        }

        final KeyIterator iterator3 = levelStorageManager.iterator(null, null);
        while (iterator3.isValid()) {
            final KeyValue keyValue = iterator3.value();
            System.out.println(keyValue);
            //assert new String(keyValue.getKey().getUserKey()).substring(1).equals(new String(keyValue.getValue().value()).substring(1));
            iterator3.nextUserKey();
        }
    }


    @Test
    public void test12() throws Exception {
//       LevelCompaction levelCompaction = new LevelCompaction();
    }

}
