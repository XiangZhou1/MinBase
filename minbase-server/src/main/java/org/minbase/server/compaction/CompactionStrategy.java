package org.minbase.server.compaction;

public enum CompactionStrategy {
    LEVEL_COMPACTION("level"),
    TIERED_COMPACTION("tiered");
    String strategy;

    CompactionStrategy(String strategy) {
        this.strategy = strategy;
    }

    @Override
    public String toString() {
        return strategy;
    }
}
