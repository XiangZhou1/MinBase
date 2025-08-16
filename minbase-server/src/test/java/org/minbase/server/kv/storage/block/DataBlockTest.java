package org.minbase.server.kv.storage.block;


import org.junit.Test;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.storage.DataBlockIterator;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.utils.KeyUtil;
import org.minbase.server.kv.utils.ValueUtil;
import org.minbase.common.utils.Util;

import java.nio.charset.StandardCharsets;


public class DataBlockTest {

    @Test
    public void blockEndCodeDecodeTest() {
        DataBlockBuilder blockBuilder = new DataBlockBuilder();
        blockBuilder.add(new KeyValue(new Key("k1".getBytes(), 1), ValueUtil.Put("v1".getBytes())));
        blockBuilder.add(new KeyValue(new Key("k2".getBytes(), 1), ValueUtil.Put("v2".getBytes())));
        blockBuilder.add(new KeyValue(new Key("k3".getBytes(), 1), ValueUtil.Put("v3".getBytes())));
        blockBuilder.add(new KeyValue(new Key("k4".getBytes(), 1), ValueUtil.Put("v4".getBytes())));
        blockBuilder.add(new KeyValue(new Key("k5".getBytes(), 1), ValueUtil.Put("v5".getBytes())));

        DataBlock block = blockBuilder.build();
        System.out.println(new String(block.encode()));

        DataBlock block1 = new DataBlock();
        block1.setKeyValueCount(5);
        block1.decode(block.encode());

        assert ByteUtil.ByteEqual(block.encode(), block1.encode());

        int keyValueNum = block1.getKeyValueCount();
        System.out.println(keyValueNum);

        DataBlockIterator dataBlockIterator = new DataBlockIterator(block1);
        while (dataBlockIterator.hasNext()) {
            KeyValue value = dataBlockIterator.next();
            System.out.println(value);
            //blockIterator.nextInnerKey();
        }
    }

    @Test
    public void blockSeekTest1() {
        DataBlockBuilder blockBuilder = new DataBlockBuilder();
        blockBuilder.add(new KeyValue(new Key("k1".getBytes(), 1), ValueUtil.Put("v1".getBytes())));
        blockBuilder.add(new KeyValue(new Key("k2".getBytes(), 1), ValueUtil.Put("v2".getBytes())));
        blockBuilder.add(new KeyValue(new Key("k3".getBytes(), 1), ValueUtil.Put("v3".getBytes())));
        blockBuilder.add(new KeyValue(new Key("k4".getBytes(), 1), ValueUtil.Put("v4".getBytes())));
        blockBuilder.add(new KeyValue(new Key("k5".getBytes(), 1), ValueUtil.Put("v5".getBytes())));

        DataBlock block = blockBuilder.build();
        DataBlock block1 = new DataBlock();
        block1.setKeyValueCount(5);
        block1.decode(block.encode());

        assert ByteUtil.ByteEqual(block.encode(), block1.encode());

        int keyValueNum = block1.getKeyValueCount();

        DataBlockIterator dataBlockIterator = new DataBlockIterator(block1, null, null);
        int i = dataBlockIterator.binarySearchFirstGreatOrEqualKey(KeyUtil.latestVersionKey("k1".getBytes(StandardCharsets.UTF_8)));
        assert i == 0;

        i = dataBlockIterator.binarySearchFirstGreatOrEqualKey(KeyUtil.latestVersionKey("k2".getBytes(StandardCharsets.UTF_8)));
        assert i == 1;

        i = dataBlockIterator.binarySearchFirstGreatOrEqualKey(KeyUtil.latestVersionKey("k3".getBytes(StandardCharsets.UTF_8)));
        assert i == 2;

        i = dataBlockIterator.binarySearchFirstGreatOrEqualKey(KeyUtil.latestVersionKey("k4".getBytes(StandardCharsets.UTF_8)));
        assert i == 3;

        i = dataBlockIterator.binarySearchFirstGreatOrEqualKey(KeyUtil.latestVersionKey("k5".getBytes(StandardCharsets.UTF_8)));
        assert i == 4;
    }


    @Test
    public void blockSeekTest3() {
        DataBlockBuilder blockBuilder = new DataBlockBuilder();
        blockBuilder.add(new KeyValue(new Key("k1".getBytes(), 1), ValueUtil.Put("v1".getBytes())));
        blockBuilder.add(new KeyValue(new Key("k2".getBytes(), 1), ValueUtil.Put("v2".getBytes())));
        blockBuilder.add(new KeyValue(new Key("k3".getBytes(), 1), ValueUtil.Put("v3".getBytes())));
        blockBuilder.add(new KeyValue(new Key("k4".getBytes(), 1), ValueUtil.Put("v4".getBytes())));
        blockBuilder.add(new KeyValue(new Key("k5".getBytes(), 1), ValueUtil.Put("v5".getBytes())));

        DataBlock block = blockBuilder.build();
        DataBlock block1 = new DataBlock();
        block1.setKeyValueCount(5);
        block1.decode(block.encode());

        assert ByteUtil.ByteEqual(block.encode(), block1.encode());

        int keyValueNum = block1.getKeyValueCount();

        DataBlockIterator dataBlockIterator = new DataBlockIterator(block1, KeyUtil.latestVersionKey("k1".getBytes(StandardCharsets.UTF_8)), KeyUtil.latestVersionKey("k3".getBytes(StandardCharsets.UTF_8)));
        while (dataBlockIterator.hasNext()) {
            KeyValue value = dataBlockIterator.next();
            System.out.println(value);
            //blockIterator.nextInnerKey();
        }
        System.out.println("======");

        DataBlockIterator dataBlockIterator2 = new DataBlockIterator(block1, KeyUtil.latestVersionKey("k2".getBytes(StandardCharsets.UTF_8)), KeyUtil.latestVersionKey("k3".getBytes(StandardCharsets.UTF_8)));
        while (dataBlockIterator2.hasNext()) {
            KeyValue value = dataBlockIterator2.next();
            System.out.println(value);
            //blockIterator.nextInnerKey();
        }
        System.out.println("======");

        DataBlockIterator dataBlockIterator3 = new DataBlockIterator(block1, KeyUtil.latestVersionKey("k3".getBytes(StandardCharsets.UTF_8)), KeyUtil.latestVersionKey("k3".getBytes(StandardCharsets.UTF_8)));
        while (dataBlockIterator3.hasNext()) {
            KeyValue value = dataBlockIterator3.next();
            System.out.println(value);
            //blockIterator.nextInnerKey();
        }
        System.out.println("======");

        DataBlockIterator dataBlockIterator4 = new DataBlockIterator(block1, KeyUtil.latestVersionKey("k4".getBytes(StandardCharsets.UTF_8)), KeyUtil.latestVersionKey("k3".getBytes(StandardCharsets.UTF_8)));
        while (dataBlockIterator4.hasNext()) {
            KeyValue value = dataBlockIterator4.next();
            System.out.println(value);
            //blockIterator.nextInnerKey();
        }
        System.out.println("======");

        DataBlockIterator dataBlockIterator5 = new DataBlockIterator(block1, KeyUtil.latestVersionKey("k5".getBytes(StandardCharsets.UTF_8)), null);
        while (dataBlockIterator5.hasNext()) {
            KeyValue value = dataBlockIterator5.next();
            System.out.println(value);
            //blockIterator.nextInnerKey();
        }
        System.out.println("======");
    }


    @Test
    public void blockSeekTest2() {
        DataBlockBuilder blockBuilder = new DataBlockBuilder();
        blockBuilder.add(new KeyValue(new Key("k1".getBytes(), 1), ValueUtil.Put("v1".getBytes())));
        blockBuilder.add(new KeyValue(new Key("k2".getBytes(), 1), ValueUtil.Put("v2".getBytes())));
        blockBuilder.add(new KeyValue(new Key("k3".getBytes(), 1), ValueUtil.Put("v3".getBytes())));
        blockBuilder.add(new KeyValue(new Key("k4".getBytes(), 1), ValueUtil.Put("v4".getBytes())));
        blockBuilder.add(new KeyValue(new Key("k5".getBytes(), 1), ValueUtil.Put("v5".getBytes())));

        DataBlock block = blockBuilder.build();
        DataBlock block1 = new DataBlock();
        block1.setKeyValueCount(5);
        block1.decode(block.encode());

        assert ByteUtil.ByteEqual(block.encode(), block1.encode());

        int keyValueNum = block1.getKeyValueCount();

        DataBlockIterator dataBlockIterator = new DataBlockIterator(block1, KeyUtil.latestVersionKey("k1".getBytes(StandardCharsets.UTF_8)), null);
        while (dataBlockIterator.hasNext()) {
            KeyValue value = dataBlockIterator.next();
            System.out.println(value);
            //blockIterator.nextInnerKey();
        }
        System.out.println("======");

        DataBlockIterator dataBlockIterator2 = new DataBlockIterator(block1, KeyUtil.latestVersionKey("k2".getBytes(StandardCharsets.UTF_8)), null);
        while (dataBlockIterator2.hasNext()) {
            KeyValue value = dataBlockIterator2.next();
            System.out.println(value);
            //blockIterator.nextInnerKey();
        }
        System.out.println("======");

        DataBlockIterator dataBlockIterator3 = new DataBlockIterator(block1, KeyUtil.latestVersionKey("k3".getBytes(StandardCharsets.UTF_8)), null);
        while (dataBlockIterator3.hasNext()) {
            KeyValue value = dataBlockIterator3.next();
            System.out.println(value);
            //blockIterator.nextInnerKey();
        }
        System.out.println("======");

        DataBlockIterator dataBlockIterator4 = new DataBlockIterator(block1, KeyUtil.latestVersionKey("k4".getBytes(StandardCharsets.UTF_8)), null);
        while (dataBlockIterator4.hasNext()) {
            KeyValue value = dataBlockIterator4.next();
            System.out.println(value);
            //blockIterator.nextInnerKey();
        }
        System.out.println("======");

        DataBlockIterator dataBlockIterator5 = new DataBlockIterator(block1, KeyUtil.latestVersionKey("k5".getBytes(StandardCharsets.UTF_8)), null);
        while (dataBlockIterator5.hasNext()) {
            KeyValue value = dataBlockIterator5.next();
            System.out.println(value);
            //blockIterator.nextInnerKey();
        }
        System.out.println("======");
    }


}
