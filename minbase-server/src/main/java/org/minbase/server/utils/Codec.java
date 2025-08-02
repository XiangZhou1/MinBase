package org.minbase.server.utils;

public interface Codec {
    byte[] encode();

    void decode(byte[] val);
}
