package org.minbase.server.compaction;

public interface Compaction {
    void compact() throws Exception;

    boolean needCompact();
}
