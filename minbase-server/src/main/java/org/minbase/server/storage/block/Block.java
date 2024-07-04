package org.minbase.server.storage.block;

public abstract class Block {

    private String blockId;

    private boolean cached;

    public String getBlockId() {
        return blockId;
    }

    public void setBlockId(String blockId) {
        this.blockId = blockId;
    }

    public boolean isCached() {
        return cached;
    }

    public void setCached(boolean cached) {
        this.cached = cached;
    }

    public abstract long length();
}
