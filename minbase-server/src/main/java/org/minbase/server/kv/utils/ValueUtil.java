package org.minbase.server.kv.utils;

import org.minbase.server.kv.Op;
import org.minbase.server.kv.Value;

public class ValueUtil {

    public static Value Delete() {
        return new Value(Op.DELETE);
    }
}
