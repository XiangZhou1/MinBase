package org.minbase.server.kv;

import org.junit.Test;
import org.minbase.server.kv.utils.ValueUtil;

import java.nio.charset.StandardCharsets;

public class KeyValueTest {
    private static final byte[] column = "cl1".getBytes(StandardCharsets.UTF_8);

    @Test
    public void keyTest1() {
        Key key = new Key("k1".getBytes(), 1);
        System.out.println(key.length());
        assert key.length() == 10;

        Key key2 = new Key();
        System.out.println(new String(key.encode()));

        key2.decode(key.encode());
        System.out.println(key);
        System.out.println(key2);
        assert key.getVersion() == key2.getVersion();
        assert key.toString().equals(key2.toString());
    }

    @Test
    public void valueTest1() {
        Value put = ValueUtil.Put("v1".getBytes());
        Value put2 = new Value();
        put2.decode(put.encode());
        System.out.println(put);
        System.out.println(put2);
        assert put.toString().equals(put2.toString());
    }

    @Test
    public void valueTest2() {
        Value put = ValueUtil.Delete();
        Value put2 = new Value();
        put2.decode(put.encode());
        System.out.println(put);
        System.out.println(put2);
        assert put.toString().equals(put2.toString());
    }


    @Test
    public void keyValueTest() {
        Value put = ValueUtil.Put("v1".getBytes());
        Key key = new Key("k1".getBytes(), 1);
        KeyValue keyValue = new KeyValue(key, put);

        KeyValue keyValue1 = new KeyValue();
        keyValue1.decode(keyValue.encode(), 0);
        System.out.println(keyValue);
        System.out.println(keyValue1);

        assert keyValue.toString().equals(keyValue1.toString());
    }

    @Test
    public void keyValueTest2() {
        Value put = ValueUtil.Delete();
        Key key = new Key("k1".getBytes(), 1);
        KeyValue keyValue = new KeyValue(key, put);

        KeyValue keyValue1 = new KeyValue();
        keyValue1.decode(keyValue.encode(), 0);
        System.out.println(keyValue);
        System.out.println(keyValue1);

        assert keyValue.toString().equals(keyValue1.toString());
    }
}
