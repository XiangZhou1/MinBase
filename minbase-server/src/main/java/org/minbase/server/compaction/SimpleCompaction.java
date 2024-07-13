package org.minbase.server.compaction;

public class SimpleCompaction implements Compaction {
    @Override
    public void compact() throws Exception {

    }

    @Override
    public boolean shouldCompact() {
        return false;
    }
}
