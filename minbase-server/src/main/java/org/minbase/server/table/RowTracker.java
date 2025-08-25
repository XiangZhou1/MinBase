package org.minbase.server.table;

import org.minbase.common.table.Row;
import org.minbase.common.table.op.ColumnValues;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.store.Scanner;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RowTracker {
    Set<OpUtil.ByteArrayWrapper> columns = new HashSet<>();

    public RowTracker(List<byte[]> byteColumns) {
        if (byteColumns == null) {
            return;
        }
        for (byte[] byteColumn : byteColumns) {
            columns.add(new OpUtil.ByteArrayWrapper(byteColumn));
        }
    }

    public List<Row> tracker(Scanner scan, int rowCountLimit) {
        List<Row> rows = new ArrayList<>();
        byte[] currentUserKey = null;
        Row row = null;
        while (scan.hasNext() && rows.size() < rowCountLimit) {
            KeyValue keyValue2 = scan.next();
            if (keyValue2 == null) {
                continue;
            }
            TableKey tableKey = new TableKey();
            tableKey.decode(keyValue2.getKey().getInternalKey());

            // 不等, 就是下一个rowKey
            if (currentUserKey != null && !ByteUtil.byteEqual(tableKey.getKey(), currentUserKey)) {
                rows.add(row);
                currentUserKey = tableKey.getKey();
                row = new Row(new String(currentUserKey));
            } else if (currentUserKey == null) {
                currentUserKey = tableKey.getKey();
                row = new Row(new String(currentUserKey));
            }

            byte[] column = tableKey.getColumn();
            if (columns.isEmpty() || columns.contains(new OpUtil.ByteArrayWrapper(column))) {
                row.add(column, keyValue2.getValue().getValue());
            }
        }

        if (row != null && rows.size() < rowCountLimit) {
            rows.add(row);
        }
        return rows;
    }
}
