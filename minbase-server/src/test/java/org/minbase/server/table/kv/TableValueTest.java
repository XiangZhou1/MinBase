package org.minbase.server.table.kv;

import org.junit.Test;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.table.TableValue;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class TableValueTest {
    private static final byte[] column1 = "column1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] value1 = "v1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] column2 = "column2".getBytes(StandardCharsets.UTF_8);
    private static final byte[] value2 = "v2".getBytes(StandardCharsets.UTF_8);

    @Test
    public void testValuePut() {
        TableValue tableValue = TableValue.Put();
        tableValue.addColumnValue(column1, value1);
        tableValue.addColumnValue(column2, value2);

        System.out.println(tableValue);

        byte[] encode = tableValue.encode();
        assert encode.length == tableValue.length();

        System.out.println(new String(encode));

        TableValue tableValue3 = new TableValue();
        tableValue3.decode(encode);

        Map<byte[], byte[]> columnValues1 = tableValue3.getColumnValues();
        Map<byte[], byte[]> columnValues2 = tableValue.getColumnValues();

        for (Map.Entry<byte[], byte[]> entry : columnValues1.entrySet()) {
            byte[] column = entry.getKey();
            assert ByteUtil.ByteEqual(columnValues1.get(column), columnValues2.get(column));
        }
    }

    @Test
    public void testValueDeleteAll() {
        TableValue tableValue = TableValue.Delete();
        System.out.println(tableValue);

        byte[] encode = tableValue.encode();
        assert encode.length == tableValue.length();

        System.out.println(new String(encode));

        TableValue tableValue3 = new TableValue();
        tableValue3.decode(encode);

        assert tableValue3.isDelete();
    }

    @Test
    public void testValueDeleteColumn() {
        TableValue tableValue = TableValue.DeleteColumn(column1, column2);
        System.out.println(tableValue);

        byte[] encode = tableValue.encode();
        assert encode.length == tableValue.length();

        System.out.println(new String(encode));

        TableValue tableValue3 = new TableValue();
        tableValue3.decode(encode);

        assert tableValue3.isDeleteColumn();
    }
}
