package org.minbase.server.op;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RowTacker {
    private KeyValue keyValue;
    private Set<byte[]> deletedColumns;
    private boolean stop = false;

    public RowTacker() {
        keyValue = new KeyValue();
        deletedColumns = new HashSet<>();
    }

    public void track(KeyValue keyValue) {
        final Value value = keyValue.getValue();
        if (value.isDelete()) {
            stop = true;
        } else if (value.isDeleteColumn()) {
            deletedColumns.addAll(value.getColumns());
        } else {
            for (Map.Entry<byte[], byte[]> entry : value.getColumnValues().entrySet()) {
                byte[] column = entry.getKey();
                if (!deletedColumns.contains(column)) {
                    this.keyValue.getValue().addColumnValue(column, entry.getValue());
                }
            }
        }
    }

    public boolean shouldStop() {
        return stop;
    }

    public KeyValue getKeyValue() {
        return keyValue;
    }
}
