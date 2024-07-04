package org.minbase.server.op;



import org.minbase.server.constant.Constants;

import java.util.ArrayList;
import java.util.List;

public class WriteBatch {
    List<KeyValue> keyValues;

    public WriteBatch() {
        keyValues = new ArrayList<>();
    }

    public void put(byte[] key, byte[] value) {
        keyValues.add(new KeyValue(new Key(key, Constants.LATEST_VERSION), Value.Put(value)));
    }

    public void delete(byte[] key) {
        keyValues.add(new KeyValue(new Key(key, Constants.NO_VERSION), Value.Delete()));
    }

    public List<KeyValue> getKeyValues() {
        return keyValues;
    }
}
