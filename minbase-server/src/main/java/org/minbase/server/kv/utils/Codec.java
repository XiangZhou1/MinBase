package org.minbase.server.kv.utils;

public interface Codec {
    byte[] encode();

    void decode(byte[] val);
}
