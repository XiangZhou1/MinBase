package org.minbase.server.table;

import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RowTacker {
    private enum GetType {
        COLUMN,
        ALL;
    }

    // 实际值
    private KeyValue keyValue;
    // 需删除的column
    private Set<byte[]> deletedColumns;

    // 感兴趣的column, null 表示全部都要
    private Set<byte[]> interestedColumns;

    private GetType type;
    private boolean stop = false;

    public RowTacker(Key key) {
        // todo
        // this.keyValue = new KeyValue(key, TableValue.Put());
        this.keyValue = null;
        this.deletedColumns = new HashSet<>();
        this.interestedColumns = null;
        type = GetType.ALL;
    }

    public RowTacker(Key key, Set<byte[]> interestedColumns) {
        // todo
        // this.keyValue = new KeyValue(key, TableValue.Put());
        this.keyValue = null;
        this.deletedColumns = new HashSet<>();
        this.interestedColumns = interestedColumns;
        type = GetType.COLUMN;
    }

    public void track(KeyValue keyValue) {
        if (stop) {
            return;
        }

        TableValue tableValue = null; // keyValue.getValue();
        if (tableValue.isDelete()) {
            stop = true;
        } else if (tableValue.isDeleteColumn()) {
            deletedColumns.addAll(tableValue.getDeletedColumns());
        } else {
            // todo
            Map<byte[], byte[]> columnValues = null;//this.keyValue.getValue().getColumnValues();

            for (Map.Entry<byte[], byte[]> entry : tableValue.getColumnValues().entrySet()) {
                byte[] column = entry.getKey();
                if (deletedColumns.contains(column)) {
                    continue;
                }
                if (type.equals(GetType.ALL)) {
                    if (!columnValues.containsKey(column)) {
                        columnValues.put(column, entry.getValue());
                    }
                } else {
                    if (interestedColumns.contains(column)) {
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
