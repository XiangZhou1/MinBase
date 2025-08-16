package org.minbase.server.kv.utils;

import org.minbase.server.kv.Op;
import org.minbase.server.kv.Value;

public class ValueUtil {

    public static Value Delete() {
        return new Value(Op.DELETE);
    }

    public static Value Put(byte[] value) {
        return new Value(Op.PUT, value);
    }

    public static boolean isDelete(Value value) {
        return value.getOp().getOp() == Op.DELETE.getOp();
    }
}
