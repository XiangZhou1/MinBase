package org.minbase.server.kv.storage.block;


import org.minbase.server.kv.KeyValue;

public class DataBlockBuilder {
    private DataBlock block;

    public DataBlockBuilder() {
        block = new DataBlock();
    }

    public void add(KeyValue kv) {
        block.add(kv);
    }

    public boolean isEmpty() {
        return block.getKeyValueCount() == 0;
    }

    public int getKeyValueNum() {
        return block.getKeyValueCount();
    }

    public long length() {
        return block.length();
    }

    public DataBlock build() {
        return block;
    }
}
