package org.minbase.server.op;

import org.junit.Test;
import org.minbase.common.utils.ByteUtil;

import java.nio.charset.StandardCharsets;

public class ColumnValueTest {
    private static final byte[] column = "column1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] value = "v1".getBytes(StandardCharsets.UTF_8);

    @Test
    public void testEncodeDecode() {
        ColumnValue columnValue = new ColumnValue(column, value);
        System.out.println(columnValue);

        byte[] encode = columnValue.encode();
        assert encode.length == columnValue.length();

        System.out.println(new String(encode));

        ColumnValue columnValue1 = new ColumnValue();
        columnValue1.decode(encode, 0);

        assert ByteUtil.byteEqual(columnValue.column(), columnValue1.column());
        assert ByteUtil.byteEqual(columnValue.value(), columnValue1.value());

    }
}
