package org.minbase.server.storage.sstable;


import org.junit.Test;
import org.minbase.server.iterator.SSTableIterator;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.op.Value;

import java.io.FileOutputStream;
import java.io.RandomAccessFile;

public class SSTableTest {
    @Test
    public void test1() {
        SSTBuilder sstBuilder = new SSTBuilder();
        for (int i = 0; i < 40960; i++) {
            Key key = new Key(("k" + i).getBytes(), 1);
            Value put = Value.Put("v1".getBytes());
            sstBuilder.add(new KeyValue(key, put));
        }

        SSTable ssTable = sstBuilder.build();
        SSTableIterator iterator = ssTable.iterator();
        while (iterator.isValid()){
            KeyValue value = iterator.value();
            System.out.println(value);
            iterator.nextKey();
        }
    }

    @Test
    public void testSeek() {
        SSTBuilder sstBuilder = new SSTBuilder();
        for (int i = 0; i < 40960; i++) {
            Key key = new Key(("k" + fillZero(i)).getBytes(), 1);
            Value put = Value.Put("v1".getBytes());
            sstBuilder.add(new KeyValue(key, put));
        }
        SSTable ssTable = sstBuilder.build();

        for (int i = 0; i < 40960; i++) {
            SSTableIterator iterator = ssTable.iterator(Key.latestKey(("k" + fillZero(i)).getBytes()), null);
            int num = 0;
            while (iterator.isValid()) {
                KeyValue value = iterator.value();
                num++;
                iterator.nextKey();
            }
            System.out.println("num=" + num +", i=" + i);
            assert num == 40960 - i;
        }
    }

    public static String fillZero(int i) {
        String value = String.valueOf(i);
        if (value.length() < 8) {
            return "00000000".substring(value.length()) + value;
        }
        return value;
    }

    @Test
    public void testEncodeAndSave() throws Exception {
        SSTBuilder sstBuilder = new SSTBuilder();
        for (int i = 0; i < 4096; i++) {
            Key key = new Key(("k" + fillZero(i)).getBytes(), 1);
            Value put = Value.Put("v1".getBytes());
            sstBuilder.add(new KeyValue(key, put));
        }

        SSTable ssTable = sstBuilder.build();
        SSTableIterator iterator = ssTable.iterator();
        while (iterator.isValid()){
            KeyValue value = iterator.value();
            //System.out.println(value);
            iterator.nextKey();
        }

        byte[] encode = ssTable.encode();
        System.out.println(encode.length);
        try(FileOutputStream fileOutputStream = new FileOutputStream("temp")){
            fileOutputStream.write(encode);
        }

        SSTable ssTable1 = new SSTable();

        try(RandomAccessFile randomAccessFile = new RandomAccessFile("temp", "r")){
            System.out.println(randomAccessFile.length());
            //byte[] read = IOUtils.read(randomAccessFile, randomAccessFile.length());
            ssTable1.loadFromFile(randomAccessFile);
        }

        System.out.println("done");

    }





    @Test
    public void testEncodeAndSave2() throws Exception {
        SSTBuilder sstBuilder = new SSTBuilder();
        for (int i = 0; i < 4096; i++) {
            Key key = new Key(("k" + fillZero(i)).getBytes(), 1);
            Value put = Value.Put("v1".getBytes());
            sstBuilder.add(new KeyValue(key, put));
        }
        SSTable ssTable = sstBuilder.build();
        System.out.println(ssTable);
        byte[] encode = ssTable.encode();
        System.out.println(encode.length);
        try(FileOutputStream fileOutputStream = new FileOutputStream("temp")){
            fileOutputStream.write(encode);
        }

        SSTable ssTable1 = new SSTable();

        try(RandomAccessFile randomAccessFile = new RandomAccessFile("temp", "r")){
            System.out.println(randomAccessFile.length());
            //byte[] read = IOUtils.read(randomAccessFile, randomAccessFile.length());
            ssTable1.loadFromFile(randomAccessFile);
        }
        System.out.println(ssTable1);

        System.out.println("done");



//        SSTableIterator iterator = ssTable.iterator();
//        while (iterator.isValid()){
//            KeyValue value = iterator.value();
//            //System.out.println(value);
//            iterator.next();
//        }

    }
//
//    @Test
//    public void testBlockMeta() throws Exception {
//        MetaBlock blockMeta = new MetaBlock();
//        blockMeta.setFirstKey("k1".getBytes());
//        blockMeta.setLastKey("k234e".getBytes());
//        blockMeta.setOffset(1);
//
//        MetaBlock blockMeta1 = new MetaBlock();
//        blockMeta1.decode(blockMeta.encode(), 0);
//        System.out.println(blockMeta1.size() == blockMeta.size());
//        System.out.println(new String(blockMeta1.getFirstKey()));
//        System.out.println(new String(blockMeta1.getLastKey()));
//        System.out.println(blockMeta1.getOffset());
//
//    }
//
//    @Test
//    public void testEncodeAndSave2() throws Exception {
//        RandomAccessFile file = new RandomAccessFile("data/level_0/1", "r");
//        SSTable ssTable = new SSTable();
//        ssTable.setLevel(0);
//        ssTable.setSsTableId(1);
//        ssTable.loadFromFile(file);
//        file.close();
//
//        SSTableIterator iterator = ssTable.iterator();
//
//    }
}
