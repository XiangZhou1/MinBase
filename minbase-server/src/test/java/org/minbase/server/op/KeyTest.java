package org.minbase.server.op;

import org.junit.Test;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyImpl;
import org.minbase.server.table.kv.InternalKey;

import java.nio.charset.StandardCharsets;

public class KeyTest {
    @Test
    public void testEncodeDecode() {
        KeyImpl key = new KeyImpl("key".getBytes(StandardCharsets.UTF_8), 100L);
        System.out.println(key);

        byte[] encode = key.encode();
        assert encode.length == key.length();

        System.out.println(new String(encode));

        KeyImpl key2 = new KeyImpl();
        key2.decode(encode);

        assert ByteUtil.byteEqual(key.getKey(), key2.getKey());
        assert key.getSequenceId() == key2.getSequenceId();

    }

    @Test
    public void testInternalKey() {
        Key key = new InternalKey("key".getBytes(StandardCharsets.UTF_8), "column1".getBytes(), 100L);
        System.out.println(key);

        System.out.println(key.length());

        byte[] encode = key.encode();
        assert encode.length == key.length();

        System.out.println(new String(encode));

        InternalKey key2 = new InternalKey();
        key2.decode(encode);

        assert ByteUtil.byteEqual(key.getKey(), key2.getKey());
        System.out.println(key2.getUserKey());
        assert key.getSequenceId() == key2.getSequenceId();

    }

    @Test
    public void testInternalKey2() {
        InternalKey key = new InternalKey("key".getBytes(StandardCharsets.UTF_8), "column1".getBytes(), 100L);
        System.out.println(key);

        System.out.println(key.length());

        System.out.println(key.getUserKey());
        System.out.println(key.getColumn());

    }
}
