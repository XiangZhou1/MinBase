package org.minbase.server.op;

import org.junit.Test;
import org.minbase.server.kv.KeyImpl;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.Value;
import org.minbase.server.table.kv.InternalKey;
import org.minbase.server.utils.ValueUtils;

import java.nio.charset.StandardCharsets;

public class KeyValueTest {
    private static final byte[] column = "cl1".getBytes(StandardCharsets.UTF_8);

    @Test
    public void keyTest1() {
        KeyImpl key = new KeyImpl("k1".getBytes(), 1);
        System.out.println(key.length());
        assert key.length() == 10;

        KeyImpl key2 = new KeyImpl();
        System.out.println(new String(key.encode()));

        key2.decode(key.encode());
        System.out.println(key);
        System.out.println(key2);
        assert key.getSequenceId() == key2.getSequenceId();
        assert key.toString().equals(key2.toString());
    }

    @Test
    public void valueTest1() {
        Value put = ValueUtils.Put( "v1".getBytes());
        Value put2 = new Value();
        put2.decode(put.encode());
        System.out.println(put);
        System.out.println(put2);
        assert put.toString().equals(put2.toString());
    }


    @Test
    public void keyValueTest() {
        Value put = ValueUtils.Put("v1".getBytes());
        InternalKey key = new InternalKey("k1".getBytes(), "c1".getBytes(), 1L);
        KeyValue keyValue = new KeyValue(key, put);

        KeyValue keyValue1 = new KeyValue();
        keyValue1.decode(keyValue.encode(), 0);
        System.out.println(keyValue);
        System.out.println(keyValue1);

        assert keyValue.toString().equals(keyValue1.toString());
    }
}
