package org.minbase.server.table;

import org.minbase.common.table.op.ColumnValues;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.store.Scanner;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RawTracker {
    Set<OpUtil.ByteArrayWrapper> columns = new HashSet<>();

    public RawTracker(List<byte[]> byteColumns) {
        for (byte[] byteColumn : byteColumns) {
            columns.add(new OpUtil.ByteArrayWrapper(byteColumn));
        }
    }

    public ColumnValues tracker(Scanner scan) {
        ColumnValues columnValues = new ColumnValues();
        while (scan.hasNext()) {
            KeyValue keyValue = scan.next();
            if (keyValue == null) {
                continue;
            }
            TableKey tableKey = new TableKey();
            tableKey.decode(keyValue.getKey().getInternalKey());
            byte[] column = tableKey.getColumn();
            if (columns.isEmpty() || columns.contains(new OpUtil.ByteArrayWrapper(column))) {
                columnValues.set(column, keyValue.getValue().getValue());
            }
        }
        return columnValues;
    }
}
