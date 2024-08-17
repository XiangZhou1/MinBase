package org.minbase.server.op;

import org.junit.Test;
import org.minbase.common.op.Delete;
import org.minbase.common.op.Put;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.kv.KeyImpl;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.table.kv.ColumnValues;
import org.minbase.server.table.kv.RowTacker;
import org.minbase.server.utils.KeyValueUtil;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;

public class RowTrackerTest {
    private static final byte[] column1 = "column1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] value1 = "v1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] column2 = "column2".getBytes(StandardCharsets.UTF_8);
    private static final byte[] value2 = "v2".getBytes(StandardCharsets.UTF_8);
    private static final byte[] rowKey = "key1".getBytes(StandardCharsets.UTF_8);

    RowTacker rowTacker;

    @Test
    public void testGetAll() {
        rowTacker = new RowTacker(KeyImpl.latestKey(rowKey));

        Put put = new Put(rowKey, column1, value1);
        KeyValue keyValue1 = KeyValueUtil.toKeyValue(put);
        rowTacker.track(keyValue1);

        Put put2 = new Put(rowKey, column2, value2);
        KeyValue keyValue2 = KeyValueUtil.toKeyValue(put2);
        rowTacker.track(keyValue2);

        KeyValue keyValue = rowTacker.getKeyValue();
        System.out.println(keyValue);
        Value val = keyValue.getValue();
        ColumnValues columnValues = val.columnValues();

        assert ByteUtil.byteEqual(columnValues.get(column1), value1);
        assert ByteUtil.byteEqual(columnValues.get(column2), value2);
    }

    @Test
    public void testGetColumn() {
        HashSet<byte[]> set = new HashSet<>();
        set.add(column1);
        rowTacker = new RowTacker(KeyImpl.latestKey(rowKey), set);

        Put put = new Put(rowKey, column1, value1);
        KeyValue keyValue1 = KeyValueUtil.toKeyValue(put);
        rowTacker.track(keyValue1);

        Put put2 = new Put(rowKey, column2, value2);
        KeyValue keyValue2 = KeyValueUtil.toKeyValue(put2);
        rowTacker.track(keyValue2);

        KeyValue keyValue = rowTacker.getKeyValue();
        System.out.println(keyValue);
        Value val = keyValue.getValue();
        ColumnValues columnValues = val.columnValues();

        assert ByteUtil.byteEqual(columnValues.get(column1), value1);
        assert columnValues.get(column2) == null;
    }

    @Test
    public void testDeleteAll1() {
        HashSet<byte[]> set = new HashSet<>();
        set.add(column1);
        rowTacker = new RowTacker(KeyImpl.latestKey(rowKey), set);

        Put put = new Put(rowKey, column1, value1);
        KeyValue keyValue1 = KeyValueUtil.toKeyValue(put);
        rowTacker.track(keyValue1);

        Put put2 = new Put(rowKey, column2, value2);
        KeyValue keyValue2 = KeyValueUtil.toKeyValue(put2);
        rowTacker.track(keyValue2);

        Delete delete = new Delete(rowKey);
        KeyValue keyValue3 = KeyValueUtil.toKeyValue(delete);
        rowTacker.track(keyValue3);

        KeyValue keyValue = rowTacker.getKeyValue();
        System.out.println(keyValue);
        Value val = keyValue.getValue();
        ColumnValues columnValues = val.columnValues();

        assert ByteUtil.byteEqual(columnValues.get(column1), value1);
        assert ByteUtil.byteEqual(columnValues.get(column2), value2);
    }


    @Test
    public void testDeleteAll2() {
        HashSet<byte[]> set = new HashSet<>();
        set.add(column1);
        rowTacker = new RowTacker(KeyImpl.latestKey(rowKey), set);

        Delete delete = new Delete(rowKey);
        KeyValue keyValue3 = KeyValueUtil.toKeyValue(delete);
        rowTacker.track(keyValue3);

        Put put = new Put(rowKey, column1, value1);
        KeyValue keyValue1 = KeyValueUtil.toKeyValue(put);
        rowTacker.track(keyValue1);

        Put put2 = new Put(rowKey, column2, value2);
        KeyValue keyValue2 = KeyValueUtil.toKeyValue(put2);
        rowTacker.track(keyValue2);

        KeyValue keyValue = rowTacker.getKeyValue();
        System.out.println(keyValue);
        Value val = keyValue.getValue();
        ColumnValues columnValues = val.columnValues();

        assert columnValues.size() == 0;
    }


    @Test
    public void testDeleteColumn1() {
        HashSet<byte[]> set = new HashSet<>();
        set.add(column1);
        rowTacker = new RowTacker(KeyImpl.latestKey(rowKey), set);

        Delete delete = new Delete(rowKey);
        delete.addColumn(column2);
        KeyValue keyValue3 = KeyValueUtil.toKeyValue(delete);
        rowTacker.track(keyValue3);

        Put put = new Put(rowKey, column1, value1);
        KeyValue keyValue1 = KeyValueUtil.toKeyValue(put);
        rowTacker.track(keyValue1);

        Put put2 = new Put(rowKey, column2, value2);
        KeyValue keyValue2 = KeyValueUtil.toKeyValue(put2);
        rowTacker.track(keyValue2);

        KeyValue keyValue = rowTacker.getKeyValue();
        System.out.println(keyValue);
        Value val = keyValue.getValue();
        ColumnValues columnValues = val.columnValues();

        assert ByteUtil.byteEqual(columnValues.get(column1), value1);
        assert columnValues.get(column2) == null;
    }

    @Test
    public void testDeleteColumn2() {
        HashSet<byte[]> set = new HashSet<>();
        set.add(column1);
        rowTacker = new RowTacker(KeyImpl.latestKey(rowKey), set);


        Put put = new Put(rowKey, column1, value1);
        KeyValue keyValue1 = KeyValueUtil.toKeyValue(put);
        rowTacker.track(keyValue1);

        Put put2 = new Put(rowKey, column2, value2);
        KeyValue keyValue2 = KeyValueUtil.toKeyValue(put2);
        rowTacker.track(keyValue2);

        Delete delete = new Delete(rowKey);
        delete.addColumn(column2);
        KeyValue keyValue3 = KeyValueUtil.toKeyValue(delete);
        rowTacker.track(keyValue3);

        KeyValue keyValue = rowTacker.getKeyValue();
        System.out.println(keyValue);
        Value val = keyValue.getValue();
        ColumnValues columnValues = val.columnValues();


        assert ByteUtil.byteEqual(columnValues.get(column1), value1);
        assert ByteUtil.byteEqual(columnValues.get(column2), value2);
    }
}
