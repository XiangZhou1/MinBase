package org.minbase.server.storage.compaction;

public enum CompactionStrategy {
    LEVEL_COMPACTION("level");

    String strategy;

    CompactionStrategy(String strategy) {
        this.strategy = strategy;
    }
}
