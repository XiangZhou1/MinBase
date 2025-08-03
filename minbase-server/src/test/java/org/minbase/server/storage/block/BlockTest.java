package org.minbase.server.storage.block;


import org.junit.Test;
import org.minbase.server.kv.storage.BlockIterator;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.storage.block.DataBlock;
import org.minbase.server.kv.storage.block.DataBlockBuilder;
import org.minbase.server.table.TableValue;
import org.minbase.common.utils.Util;

import java.nio.charset.StandardCharsets;


public class BlockTest {
    private static final byte[] column = "cl1".getBytes(StandardCharsets.UTF_8);

    @Test
    public void blockEndCodeDecodeTest() {
        DataBlockBuilder blockBuilder = new DataBlockBuilder();
        blockBuilder.add(new KeyValue(new Key("k1".getBytes(), 1), TableValue.Put(column, "v1".getBytes())));
        blockBuilder.add(new KeyValue(new Key("k2".getBytes(), 1), TableValue.Put(column, "v2".getBytes())));
        blockBuilder.add(new KeyValue(new Key("k3".getBytes(), 1), TableValue.Put(column, "v3".getBytes())));
        blockBuilder.add(new KeyValue(new Key("k4".getBytes(), 1), TableValue.Put(column, "v4".getBytes())));
        blockBuilder.add(new KeyValue(new Key("k5".getBytes(), 1), TableValue.Put(column, "v5".getBytes())));

        DataBlock block = blockBuilder.build();
        System.out.println(new String(block.encode()));

        DataBlock block1 = new DataBlock();
        block1.setKeyValueCount(5);
        block1.decode(block.encode());

        int keyValueNum = block1.getKeyValueCount();
        System.out.println(keyValueNum);

        BlockIterator blockIterator = new BlockIterator(block1);
        while (blockIterator.hasNext()) {
            KeyValue value = blockIterator.value();
            System.out.println(value);
            blockIterator.nextInnerKey();
        }
    }


    @Test
    public void blockIterTest(){
        int totalnum = 1000;
        DataBlockBuilder blockBuilder = new DataBlockBuilder();
        for (int i=0; i<totalnum; i++) {
            blockBuilder.add(new KeyValue(new Key(("k" + Util.fillZero(i)).getBytes(), 3), TableValue.Put(column, ("v" + i).getBytes())));
            blockBuilder.add(new KeyValue(new Key(("k" + Util.fillZero(i)).getBytes(), 2), TableValue.Put(column, ("v" + i).getBytes())));
            blockBuilder.add(new KeyValue(new Key(("k" + Util.fillZero(i)).getBytes(), 1), TableValue.Put(column, ("v" + i).getBytes())));
        }

        DataBlock block = blockBuilder.build();

        for (int i=0; i<totalnum; i++){
            BlockIterator blockIterator = new BlockIterator(block, Key.latestKey(("k"+ Util.fillZero(i)).getBytes()), null);
            int num = 0;
            while (blockIterator.hasNext()) {
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
        for (int i=0; i<totalnum; i++) {
            blockBuilder.add(new KeyValue(new Key(("k" + Util.fillZero(i)).getBytes(), 3), TableValue.Put(column, ("v" + i).getBytes())));
            blockBuilder.add(new KeyValue(new Key(("k" + Util.fillZero(i)).getBytes(), 2), TableValue.Put(column, ("v" + i).getBytes())));
            blockBuilder.add(new KeyValue(new Key(("k" + Util.fillZero(i)).getBytes(), 1), TableValue.Put(column, ("v" + i).getBytes())));
        }

        DataBlock block = blockBuilder.build();

        for (int i=0; i<totalnum; i++){
            BlockIterator blockIterator = new BlockIterator(block, Key.latestKey(("k"+ Util.fillZero(i)).getBytes()), null);
            int num = 0;
            while (blockIterator.hasNext()) {
                KeyValue value = blockIterator.value();
                num ++;
                blockIterator.next();
            }
            System.out.println("i=" + i + ", num=" + num);
            assert num == (totalnum - i);
        }
    }



}
