package org.minbase.server.kv;

import org.junit.Test;
import org.minbase.common.utils.ByteUtil;

import java.nio.charset.StandardCharsets;

public class KeyTest {
    @Test
    public void testEncodeDecode() {
        Key key = new Key("key".getBytes(StandardCharsets.UTF_8), 100L);
        System.out.println(key);

        byte[] encode = key.encode();
        assert encode.length == key.length();

        System.out.println(new String(encode));

        Key key2 = new Key();
        key2.decode(encode);
        System.out.println(key2);

        assert ByteUtil.byteEqual(key.getInternalKey(), key2.getInternalKey());
        assert key.getVersion() == key2.getVersion();
    }

    @Test
    public void testCompare() {
        Key key = new Key("key".getBytes(StandardCharsets.UTF_8), 100L);
        Key key2 = new Key("key".getBytes(StandardCharsets.UTF_8), 1000L);
        assert key.compareTo(key2) > 0;

        Key key3 = new Key("key".getBytes(StandardCharsets.UTF_8), 10000L);
        Key key4 = new Key("key".getBytes(StandardCharsets.UTF_8), 10000L);
        assert key3.compareTo(key4) == 0;
    }
}
