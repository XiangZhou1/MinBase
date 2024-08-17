package org.minbase.server.utils;

import org.minbase.server.kv.Value;

import static org.minbase.server.kv.Value.TYPE_DELETE;
import static org.minbase.server.kv.Value.TYPE_PUT;

public class ValueUtils {
    private static final Value DELETE = new Value(TYPE_DELETE);

    public static Value Delete() {
        return DELETE;
    }

    public static Value Put(byte[] data) {
        return new Value(TYPE_PUT, data);
    }
}
