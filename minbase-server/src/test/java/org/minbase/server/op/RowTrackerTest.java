package org.minbase.server.op;

import org.junit.Test;
import org.minbase.common.op.Delete;
import org.minbase.common.op.Put;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.kv.KeyImpl;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.table.kv.ColumnValues;
import org.minbase.server.table.kv.InternalKey;
import org.minbase.server.table.kv.RowTacker;
import org.minbase.server.utils.KeyUtils;
import org.minbase.server.utils.KeyValueUtil;
import org.minbase.server.utils.ValueUtils;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;

public class RowTrackerTest {
    private static final byte[] column1 = "column1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] value1 = "v1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] column2 = "column2".getBytes(StandardCharsets.UTF_8);
    private static final byte[] value2 = "v2".getBytes(StandardCharsets.UTF_8);
    private static final byte[] rowKey = "key1".getBytes(StandardCharsets.UTF_8);

    RowTacker rowTacker;

    @Test
    public void testGetAll() {
        HashSet<String> set = new HashSet<>();
        set.add(new String(column1));
        rowTacker = new RowTacker(new String(rowKey), set, 20L);

        KeyValue keyValue1 = new KeyValue(new InternalKey(rowKey, column1, 1), ValueUtils.Put(value1));
        rowTacker.track(keyValue1);

        KeyValue keyValue2 = new KeyValue(new InternalKey(rowKey, column2, 1), ValueUtils.Put(value2));
        rowTacker.track(keyValue2);

        List<KeyValue> keyValues = rowTacker.getKeyValues();
        for (KeyValue keyValue : keyValues) {
            System.out.println(keyValue);
        }
    }

    @Test
    public void testGetAll2() {
        HashSet<String> set = new HashSet<>();
        set.add(new String(column1));
        rowTacker = new RowTacker(new String(rowKey), set, 20L);

        KeyValue keyValue1 = new KeyValue(new InternalKey(rowKey, column1, 2), ValueUtils.Put(value1));
        rowTacker.track(keyValue1);

        KeyValue keyValue2 = new KeyValue(new InternalKey(rowKey, column1, 1), ValueUtils.Put(value1));
        rowTacker.track(keyValue2);

        List<KeyValue> keyValues = rowTacker.getKeyValues();
        for (KeyValue keyValue : keyValues) {
            System.out.println(keyValue);
        }
    }
    @Test
    public void testGetAll3() {
        HashSet<String> set = new HashSet<>();
        set.add(new String(column1));
        set.add(new String(column2));
        rowTacker = new RowTacker(new String(rowKey), set, 20L);

        KeyValue keyValue1 = new KeyValue(new InternalKey(rowKey, column1, 1), ValueUtils.Put(value1));
        rowTacker.track(keyValue1);

        KeyValue keyValue2 = new KeyValue(new InternalKey(rowKey, column2, 1), ValueUtils.Put(value2));
        rowTacker.track(keyValue2);

        List<KeyValue> keyValues = rowTacker.getKeyValues();
        for (KeyValue keyValue : keyValues) {
            System.out.println(keyValue);
        }
    }

    @Test
    public void testGetAll4() {
        HashSet<String> set = new HashSet<>();
        set.add(new String(column1));
        set.add(new String(column2));
        rowTacker = new RowTacker(new String(rowKey), set, 20L);

        KeyValue keyValue1 = new KeyValue(new InternalKey(rowKey, column1, 2), ValueUtils.Put(value1));
        rowTacker.track(keyValue1);

        KeyValue keyValue2 = new KeyValue(new InternalKey(rowKey, column2, 2), ValueUtils.Delete());
        rowTacker.track(keyValue2);

        KeyValue keyValue3 = new KeyValue(new InternalKey(rowKey, column2, 2), ValueUtils.Put(value2));
        rowTacker.track(keyValue3);

        List<KeyValue> keyValues = rowTacker.getKeyValues();
        for (KeyValue keyValue : keyValues) {
            System.out.println(keyValue);
        }
    }

}
