package org.minbase.server.op;

import org.junit.Test;
import org.minbase.common.utils.ByteUtil;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ValueTest {
    private static final byte[] column1 = "column1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] value1 = "v1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] column2 = "column2".getBytes(StandardCharsets.UTF_8);
    private static final byte[] value2 = "v2".getBytes(StandardCharsets.UTF_8);

    @Test
    public void testValuePut() {
        Value value = Value.Put();
        value.addColumnValue(column1, value1);
        value.addColumnValue(column2, value2);

        System.out.println(value);

        byte[] encode = value.encode();
        assert encode.length == value.length();

        System.out.println(new String(encode));

        Value value3 = new Value();
        value3.decode(encode);

        Map<byte[], byte[]> columnValues1 = value3.getColumnValues();
        Map<byte[], byte[]> columnValues2 = value.getColumnValues();

        for (Map.Entry<byte[], byte[]> entry : columnValues1.entrySet()) {
            byte[] column = entry.getKey();
            assert ByteUtil.byteEqual(columnValues1.get(column), columnValues2.get(column));
        }
    }

    @Test
    public void testValueDeleteAll() {
        Value value = Value.Delete();
        System.out.println(value);

        byte[] encode = value.encode();
        assert encode.length == value.length();

        System.out.println(new String(encode));

        Value value3 = new Value();
        value3.decode(encode);

        assert value3.isDelete();
    }

    @Test
    public void testValueDeleteColumn() {
        Value value = Value.DeleteColumn(column1, column2);
        System.out.println(value);

        byte[] encode = value.encode();
        assert encode.length == value.length();

        System.out.println(new String(encode));

        Value value3 = new Value();
        value3.decode(encode);

        assert value3.isDeleteColumn();
    }
}
