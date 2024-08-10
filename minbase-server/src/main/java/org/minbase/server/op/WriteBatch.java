package org.minbase.server.op;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WriteBatch {
    Map<String, List<KeyValue>> keyValues;

    public WriteBatch() {
        keyValues = new HashMap<>();
    }

    public void add(String tableName, KeyValue keyValue) {
        List<KeyValue> keyValues = this.keyValues.get(tableName);
        if (keyValue == null) {
            keyValues = new ArrayList<>();
            this.keyValues.put(tableName, keyValues);
        }
        keyValues.add(keyValue);
    }

    public List<KeyValue> getKeyValues(String tableName) {
        return this.keyValues.get(tableName);
    }

    public void setSequenceId(long sequenceId) {
        for (List<KeyValue> keyValues : keyValues.values()) {
            for (KeyValue keyValue : keyValues) {
                keyValue.getKey().setSequenceId(sequenceId);
            }
        }
    }

    public List<String> getTables() {
        return new ArrayList<>(keyValues.keySet());
    }

    public boolean isEmpty() {
        return keyValues.isEmpty();
    }
}
