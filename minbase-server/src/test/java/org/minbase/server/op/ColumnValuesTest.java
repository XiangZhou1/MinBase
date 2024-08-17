package org.minbase.server.op;

import org.junit.Test;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.table.kv.ColumnValues;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ColumnValuesTest {
    private static final byte[] column = "column1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] value = "v1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] column2 = "column2".getBytes(StandardCharsets.UTF_8);
    private static final byte[] value2 = "v2".getBytes(StandardCharsets.UTF_8);

    @Test
    public void testEncodeDecode() {
        ColumnValues columnValues = new ColumnValues();
        columnValues.add(column, value);
        columnValues.add(column2, value2);

        System.out.println(columnValues);

        byte[] encode = columnValues.encode();
        assert encode.length == columnValues.length();

        System.out.println(new String(encode));

        ColumnValues columnValues1 = new ColumnValues();
        columnValues1.decode(encode, 0);

        for (Map.Entry<byte[], byte[]> entry : columnValues.getColumnValues().entrySet()) {
            byte[] column = entry.getKey();
            assert ByteUtil.byteEqual(columnValues1.get(column), columnValues.get(column));
        }
    }
}
