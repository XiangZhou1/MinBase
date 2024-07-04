package org.minbase.server.storage.compaction;

public interface Compaction {
    void compact() throws Exception;

    boolean shouldCompact();
}
