package org.minbase.table;

import org.junit.Test;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.table.TableKey;

import java.nio.charset.StandardCharsets;

public class TableKeyTest {
    @Test
    public void testCondec() {
        TableKey tableKey = new TableKey("k1".getBytes(StandardCharsets.UTF_8), "c1".getBytes(StandardCharsets.UTF_8));
        byte[] encode = tableKey.encode();

        TableKey tableKey1 = new TableKey();
        tableKey1.decode(encode);

        assert ByteUtil.byteEqual(encode, tableKey1.encode());
    }
}
