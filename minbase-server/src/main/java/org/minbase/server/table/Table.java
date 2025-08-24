package org.minbase.server.table;

import org.minbase.common.exception.ServerException;
import org.minbase.common.exception.TableNotExistException;
import org.minbase.common.exception.TransactionNotExistException;
import org.minbase.common.table.op.ColumnValues;
import org.minbase.common.table.op.Delete;
import org.minbase.common.table.op.Get;
import org.minbase.common.table.op.Put;
import org.minbase.common.utils.ByteUtil;

import java.io.IOException;
import java.util.Collections;

public interface Table {
    String name();

    ColumnValues get(Get get) throws IOException;

    void put(Put put) throws IOException;

    boolean checkAndPut(byte[] checkKey, byte[] column, byte[] checkValue, Put put) throws IOException;

    void delete(Delete key) throws IOException;

}
