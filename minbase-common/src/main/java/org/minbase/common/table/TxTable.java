package org.minbase.common.table;

import org.minbase.common.exception.ServerException;
import org.minbase.common.exception.TableNotExistException;
import org.minbase.common.exception.TransactionException;
import org.minbase.common.exception.TransactionNotExistException;
import org.minbase.common.table.op.ColumnValues;
import org.minbase.common.table.op.Delete;
import org.minbase.common.table.op.Get;
import org.minbase.common.table.op.Put;
import org.minbase.common.utils.ByteUtil;

import java.util.Collections;

public interface TxTable {
    String name();

    ColumnValues get(Get get) throws TableNotExistException, ServerException, TransactionNotExistException;

    default String get(String key, String column) throws TableNotExistException, ServerException, TransactionNotExistException {
        Get get = new Get(ByteUtil.toBytes(key), Collections.singletonList(ByteUtil.toBytes(column)));
        ColumnValues columnValues = get(get);
        byte[] bytes = columnValues.get(ByteUtil.toBytes(column));
        return bytes == null ? null : new String(bytes);
    }

    void put(Put put) throws TableNotExistException, ServerException, TransactionNotExistException;


    default void put(String key, String column, String value) throws TableNotExistException, ServerException, TransactionNotExistException {
        Put put = new Put(ByteUtil.toBytes(key), ByteUtil.toBytes(column), ByteUtil.toBytes(value));
        put(put);
    }

    boolean checkAndPut(byte[] checkKey, byte[] column, byte[] checkValue, Put put) throws TableNotExistException, ServerException, TransactionNotExistException;
    default  boolean checkAndPut(String checkKey, String checkColumn, String checkValue,
                        String key, String column, String value) throws TableNotExistException, ServerException, TransactionNotExistException{
        Put put = new Put(ByteUtil.toBytes(key), ByteUtil.toBytes(column), ByteUtil.toBytes(value));
        return checkAndPut(ByteUtil.toBytes(checkKey), ByteUtil.toBytes(checkColumn), ByteUtil.toBytes(checkValue),  put);
    }

    void delete(Delete key) throws TableNotExistException, ServerException, TransactionNotExistException;

    default void delete(String key, String... cloumns) throws ServerException, TableNotExistException, TransactionNotExistException {
        Delete key1 = new Delete(ByteUtil.toBytes(key));
        for (String cloumn : cloumns) {
            key1.addColumn(ByteUtil.toBytes(cloumn));
        }
        delete(key1);
    }
}