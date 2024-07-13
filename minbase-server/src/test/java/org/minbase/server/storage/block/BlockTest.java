package org.minbase.server.storage.block;


import org.junit.Test;
import org.minbase.server.iterator.BlockIterator;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.op.Value;
import org.minbase.common.utils.Util;


public class BlockTest {
    @Test
    public void blockEndCodeDecodeTest(){
        DataBlockBuilder blockBuilder = new DataBlockBuilder();
        blockBuilder.add(new KeyValue(new Key("k1".getBytes(), 1), Value.Put("v1".getBytes())));
        blockBuilder.add(new KeyValue(new Key("k2".getBytes(), 1), Value.Put("v2".getBytes())));
        blockBuilder.add(new KeyValue(new Key("k3".getBytes(), 1), Value.Put("v3".getBytes())));
        blockBuilder.add(new KeyValue(new Key("k4".getBytes(), 1), Value.Put("v4".getBytes())));
        blockBuilder.add(new KeyValue(new Key("k5".getBytes(), 1), Value.Put("v5".getBytes())));

        DataBlock block = blockBuilder.build();
        System.out.println(new String(block.encode()));

        DataBlock block1 = new DataBlock();
        block1.setKeyValueNum(5);
        block1.decode(block.encode());

        int keyValueNum = block1.getKeyValueNum();
        System.out.println(keyValueNum);

        BlockIterator blockIterator = new BlockIterator(block1);
        while (blockIterator.isValid()){
            KeyValue value = blockIterator.value();
            System.out.println(value);
            blockIterator.nextInnerKey();
        }
    }


    @Test
    public void blockIterTest(){
        int totalnum = 1000;
        DataBlockBuilder blockBuilder = new DataBlockBuilder();
        for (int i=0; i<totalnum; i++){
            blockBuilder.add(new KeyValue(new Key(("k"+ Util.fillZero(i)).getBytes(), 3), Value.Put(("v"+i).getBytes())));
            blockBuilder.add(new KeyValue(new Key(("k"+ Util.fillZero(i)).getBytes(), 2), Value.Put(("v"+i).getBytes())));
            blockBuilder.add(new KeyValue(new Key(("k"+ Util.fillZero(i)).getBytes(), 1), Value.Put(("v"+i).getBytes())));
        }

        DataBlock block = blockBuilder.build();

        for (int i=0; i<totalnum; i++){
            BlockIterator blockIterator = new BlockIterator(block, Key.latestKey(("k"+ Util.fillZero(i)).getBytes()), null);
            int num = 0;
            while (blockIterator.isValid()){
                KeyValue value = blockIterator.value();
                num ++;
                blockIterator.nextInnerKey();
            }
            System.out.println("i=" + i + ", num=" + num);
            assert num == 3 * (totalnum - i);
        }
    }



    @Test
    public void blockIterTest2(){
        int totalnum = 1000;
        DataBlockBuilder blockBuilder = new DataBlockBuilder();
        for (int i=0; i<totalnum; i++){
            blockBuilder.add(new KeyValue(new Key(("k"+ Util.fillZero(i)).getBytes(), 3), Value.Put(("v"+i).getBytes())));
            blockBuilder.add(new KeyValue(new Key(("k"+ Util.fillZero(i)).getBytes(), 2), Value.Put(("v"+i).getBytes())));
            blockBuilder.add(new KeyValue(new Key(("k"+ Util.fillZero(i)).getBytes(), 1), Value.Put(("v"+i).getBytes())));
        }

        DataBlock block = blockBuilder.build();

        for (int i=0; i<totalnum; i++){
            BlockIterator blockIterator = new BlockIterator(block, Key.latestKey(("k"+ Util.fillZero(i)).getBytes()), null);
            int num = 0;
            while (blockIterator.isValid()){
                KeyValue value = blockIterator.value();
                num ++;
                blockIterator.next();
            }
            System.out.println("i=" + i + ", num=" + num);
            assert num == (totalnum - i);
        }
    }



}
