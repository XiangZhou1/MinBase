package org.minbase.server.kv;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WriteBatch {
    Map<String, List<KeyValue>> keyValues;
    private long lastSequenceId;

    public WriteBatch() {
        keyValues = new HashMap<>();
    }

    public void add(String storeName, KeyValue keyValue) {
        List<KeyValue> keyValues = this.keyValues.get(storeName);
        if (keyValues == null) {
            keyValues = new ArrayList<>();
            this.keyValues.put(storeName, keyValues);
        }
        keyValues.add(keyValue);
        this.lastSequenceId = Math.max(this.lastSequenceId, keyValue.getKey().getVersion());
    }

    public List<KeyValue> getKeyValues(String storeName) {
        return this.keyValues.get(storeName);
    }

    public void setLastSequenceId(long lastSequenceId) {
        this.lastSequenceId = lastSequenceId;
    }

    public List<String> getStoreNames() {
        return new ArrayList<>(keyValues.keySet());
    }

    public boolean isEmpty() {
        return keyValues.isEmpty();
    }

    public long getLastSequenceId() {
        return lastSequenceId;
    }
}
