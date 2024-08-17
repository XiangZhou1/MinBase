package org.minbase.common.op;

public abstract class Op {
    protected long sequenceId = 0;

    public long getSequenceId() {
        return sequenceId;
    }

    public void setSequenceId(long sequenceId) {
        this.sequenceId = sequenceId;
    }
}
