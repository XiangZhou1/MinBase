package org.minbase.server.table;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class KeySet {
    Map<String, Set<OpUtil.ByteArrayWrapper>> set = new HashMap<>();

    public void put(String table, byte[] key) {
        Set<OpUtil.ByteArrayWrapper> bytes = set.computeIfAbsent(table, k -> new HashSet<>());
        bytes.add(new OpUtil.ByteArrayWrapper(key));
    }

    public boolean isOverLap(KeySet other) {
        if (isEmpty() || other.isEmpty()) {
            return false;
        }
        for (Map.Entry<String, Set<OpUtil.ByteArrayWrapper>> entry : other.set.entrySet()) {
            String table = entry.getKey();
            Set<OpUtil.ByteArrayWrapper> keys = set.get(table);
            if (keys == null) {
                continue;
            }
            Set<OpUtil.ByteArrayWrapper> otherKeys = entry.getValue();
            for (OpUtil.ByteArrayWrapper otherKey : otherKeys) {
                if (keys.contains(otherKey)) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isEmpty() {
        return set.isEmpty();
    }

    @Override
    public String toString() {
        return "KeySet{" +
                "set=" + set +
                '}';
    }
}
