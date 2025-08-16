package org.minbase.server.kv.store;

public class MultiVersionControler {
    private volatile long readPoint = 0;
    private volatile long writePoint = 0;

    public long incrementAndGetWritePoint() {
        ++writePoint;
        return writePoint;
    }

    public void completeWrite(long sequenceId) {
        this.readPoint = sequenceId;
    }

    public long getReadPoint() {
        return readPoint;
    }

    public void setWritePoint(long sequenceId) {
        this.writePoint = sequenceId;
    }

    public long getWritePoint() {
        return this.writePoint;
    }
}
