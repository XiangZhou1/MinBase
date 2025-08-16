package org.minbase.server.kv.storage.block;

import org.minbase.common.utils.BloomFilter;
import org.minbase.server.kv.Length;

import java.util.BitSet;


public class BloomFilterBlock extends BloomFilter implements Length {
    public BloomFilterBlock() {
        super(10000, 0.01);
    }

    @Override
    public int length() {
        return bitSetSize;
    }

    public byte[] encode() {
        return bitSet.toByteArray();
    }

    public void decode(byte[] buf) {
        this.bitSet = BitSet.valueOf(buf);
    }


    @Override
    public String toString() {
        return "BloomFilterBlock{" +
                "bitSet=" + bitSet +
                '}';
    }
}
