package org.minbase.server.storage.block;


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
        return block.getKeyValueNum() == 0;
    }

    public int getKeyValueNum() {
        return block.getKeyValueNum();
    }

    public long length() {
        return block.length();
    }

    public DataBlock build() {
        return block;
    }
}
