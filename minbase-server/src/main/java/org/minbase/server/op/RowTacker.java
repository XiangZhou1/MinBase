package org.minbase.server.op;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RowTacker {
    // 实际值
    private KeyValue keyValue;
    // 需删除的column
    private Set<byte[]> deletedColumns;

    // 感兴趣的column, null 表示全部都要
    private Set<byte[]> interestedColumns;

    private boolean stop = false;

    public RowTacker(Key key, Set<byte[]> interestedColumns) {
        this.keyValue = new KeyValue(key, Value.Put());
        this.deletedColumns = new HashSet<>();
        this.interestedColumns = interestedColumns;
    }

    public void track(KeyValue keyValue) {
        final Value value = keyValue.getValue();
        if (value.isDelete()) {
            stop = true;
        } else if (value.isDeleteColumn()) {
            deletedColumns.addAll(value.getDeletedColumns());
        } else {
            for (Map.Entry<byte[], byte[]> entry : value.getColumnValues().entrySet()) {
                byte[] column = entry.getKey();
                if (!deletedColumns.contains(column)) {
                    if (interestedColumns == null || interestedColumns.contains(column)) {
                        Map<byte[], byte[]> columnValues = this.keyValue.getValue().getColumnValues();
                        if (!columnValues.containsKey(column)) {
                            columnValues.put(column, entry.getValue());
                        }
                    }
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
