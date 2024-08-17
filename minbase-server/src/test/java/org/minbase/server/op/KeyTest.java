package org.minbase.server.op;

import org.junit.Test;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.kv.KeyImpl;

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

        assert ByteUtil.byteEqual(key.getUserKey(), key2.getUserKey());
        assert key.getSequenceId() == key2.getSequenceId();

    }
}
