package org.minbase.server.table.kv;

import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class RowTacker {

    private final String userKey;
    private final long snapShot;
    // 实际值
    private final List<KeyValue> keyValues;

    // 感兴趣的column, null 表示全部都要
    private final Set<String> interestedColumns;

    private boolean stop = false;


    public RowTacker(String userKey, Set<String> interestedColumns, long snapShot) {
        this.userKey = userKey;
        this.interestedColumns = interestedColumns;
        this.keyValues = new ArrayList<>();
        this.snapShot = snapShot;
    }

    public void track(KeyValue keyValue) {
        if (stop) {
            return;
        }
        InternalKey key = (InternalKey) keyValue.getKey();
        if (key.getSequenceId() > snapShot) {
            return;
        }

        Value value = keyValue.getValue();
        if (value.isDelete()) {
            interestedColumns.remove(key.getColumn());
            if (interestedColumns.isEmpty()) {
                stop = true;
            }
        } else {
            if (interestedColumns.contains(key.getColumn())) {
                keyValues.add(keyValue);
                interestedColumns.remove(key.getColumn());
                if (interestedColumns.isEmpty()) {
                    stop = true;
                }
            }
        }
    }

    public boolean shouldStop() {
        return stop;
    }

    public List<KeyValue> getKeyValues() {
        return keyValues;
    }
}
