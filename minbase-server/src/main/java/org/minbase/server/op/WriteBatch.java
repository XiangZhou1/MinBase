package org.minbase.server.op;



import org.minbase.server.constant.Constants;

import java.util.ArrayList;
import java.util.List;

public class WriteBatch {
    List<KeyValue> keyValues;

    public WriteBatch() {
        keyValues = new ArrayList<>();
    }

    public void add(KeyValue keyValue) {
        keyValues.add(keyValue);
    }

    public List<KeyValue> getKeyValues() {
        return keyValues;
    }
}
