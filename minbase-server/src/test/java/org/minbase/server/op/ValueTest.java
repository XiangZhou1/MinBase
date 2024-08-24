package org.minbase.server.op;

import org.junit.Test;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.kv.Value;
import org.minbase.server.utils.ValueUtils;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ValueTest {

    @Test
    public void testValuePut() {
        Value value = ValueUtils.Put("v1".getBytes());
        System.out.println(value);

        byte[] encode = value.encode();
        assert encode.length == value.length();

        System.out.println(new String(encode));

        Value value2 = new Value();
        value2.decode(encode);
        System.out.println(value2);
        assert value.toString().equals(value2.toString());
    }

}
