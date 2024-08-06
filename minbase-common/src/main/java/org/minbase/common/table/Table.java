package org.minbase.common.table;

import org.minbase.common.operation.Delete;
import org.minbase.common.operation.Get;
import org.minbase.common.operation.Put;
import org.minbase.common.operation.ColumnValues;
import org.minbase.common.utils.ByteUtil;

import java.util.Collections;

public interface Table {
    String name();

    ColumnValues get(Get get);

    default byte[] get(byte[] key, String column) {
        Get get = new Get(key, Collections.singletonList(ByteUtil.toBytes(column)));
        ColumnValues columnValues = get(get);
        return columnValues.get(ByteUtil.toBytes(column));
    }

    void put(Put put);

    default void put(byte[] key, String column, byte[] value) {
        Put put = new Put(key, ByteUtil.toBytes(column), value);
        put(put);
    }

    boolean checkAndPut(byte[] checkKey, byte[] column, byte[] checkValue, Put put);

    void delete(Delete key);

    default void delete(byte[] key) {
        delete(new Delete(key));
    }

}
