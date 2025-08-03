package org.minbase.server.kv.storage.block;

import org.minbase.server.kv.Length;

public abstract class Block implements Length {

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

}
