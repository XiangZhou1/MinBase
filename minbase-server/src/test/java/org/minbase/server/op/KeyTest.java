package org.minbase.server.op;

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

        assert ByteUtil.byteEqual(key.getUserKey(), key2.getUserKey());
        assert key.getSequenceId() == key2.getSequenceId();

    }
}
