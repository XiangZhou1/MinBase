package org.minbase.server.storage.compaction;

public enum CompactionStrategy {
    LEVEL_COMPACTION("level");

    String strategy;

    @Override
    public String toString() {
         return strategy;
    }

    CompactionStrategy(String strategy) {
        this.strategy = strategy;
    }
}
