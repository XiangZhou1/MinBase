package org.minbase.server.kv.utils;

import java.io.IOException;
import java.io.OutputStream;

public interface Codec {
    byte[] encode();

    default int encodeToStream(OutputStream outputStream) throws IOException {
        return 0;
    }

    default void decode(byte[] val) {

    }

    default void decode(byte[] val, int offset) {

    }
}
