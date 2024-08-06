package org.minbase.server.table;

import org.minbase.common.operation.Delete;
import org.minbase.common.operation.Get;
import org.minbase.common.operation.Put;
import org.minbase.common.operation.ColumnValues;
import org.minbase.common.table.Table;


public class TransactionTable implements Table {
    @Override
    public String name() {
        return null;
    }

    @Override
    public ColumnValues get(Get get) {
        return null;
    }

    @Override
    public void put(Put put) {

    }

    @Override
    public boolean checkAndPut(byte[] checkKey, byte[] column, byte[] checkValue, Put put) {
        return false;
    }

    @Override
    public void delete(Delete key) {

    }
}
