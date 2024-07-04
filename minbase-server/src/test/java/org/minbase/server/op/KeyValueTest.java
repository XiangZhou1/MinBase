package org.minbase.server.op;

import org.junit.Test;

public class KeyValueTest {

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
        assert key.getSequenceId() == key2.getSequenceId();
        assert key.toString().equals(key2.toString());
    }

    @Test
    public void valueTest1() {
        Value put = Value.Put("v1".getBytes());
        Value put2 = new Value();
        put2.decode(put.encode());
        System.out.println(put);
        System.out.println(put2);
        assert put.toString().equals(put2.toString());
    }


    @Test
    public void keyValueTest() {
        Value put = Value.Put("v1".getBytes());
        Key key = new Key("k1".getBytes(), 1);
        KeyValue keyValue = new KeyValue(key, put);

        KeyValue keyValue1 = new KeyValue();
        keyValue1.decode(keyValue.encode(), 0);
        System.out.println(keyValue);
        System.out.println(keyValue1);

        assert keyValue.toString().equals(keyValue1.toString());
    }
}
