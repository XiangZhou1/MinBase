package org.minbase.server.table;

import org.minbase.common.operation.Delete;
import org.minbase.common.operation.Get;
import org.minbase.common.operation.Put;
import org.minbase.common.operation.ColumnValues;
import org.minbase.common.table.*;
import org.minbase.server.constant.Constants;
import org.minbase.server.minstore.MinStore;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.op.Value;
import org.minbase.server.op.WriteBatch;

import java.util.List;
import java.util.Map;

public class TableImpl implements Table {
    String tableName;
    MinStore minStore;

    public TableImpl(String tableName, MinStore minStore) {
        this.tableName = tableName;
        this.minStore = minStore;
    }


    public MinStore getMinStore() {
        return minStore;
    }

    @Override
    public String name() {
        return tableName;
    }

    @Override
    public ColumnValues get(Get get) {
        KeyValue keyValue = minStore.get(get);
        return keyValue.getValue().columnValues();
    }

    @Override
    public void put(Put put) {
        WriteBatch writeBatch = new WriteBatch();
        for (Map.Entry<byte[], byte[]> entry : put.getColumnValues().entrySet()) {
            Key key = new Key(put.getKey(), Constants.NO_VERSION);
            Value value = Value.Put(new Coentry.getValue());
            writeBatch.add(new KeyValue(key, value));
        }
        minStore.put(writeBatch);
    }

    @Override
    public boolean checkAndPut(byte[] checkKey, byte[] column, byte[] checkValue, Put put) {
        return false;
    }

    @Override
    public void delete(Delete delete) {
        WriteBatch writeBatch = new WriteBatch();
        final List<byte[]> columns = delete.getColumns();
        if (columns.isEmpty()) {
            Key key = new Key(delete.getKey(), null, Constants.NO_VERSION);
            Value value = Value.Delete();
            writeBatch.add(new KeyValue(key, value));
        } else {
            for (byte[] column : columns) {
                Key key = new Key(delete.getKey(), column, Constants.NO_VERSION);
                Value value = Value.DeleteColumn();
                writeBatch.add(new KeyValue(key, value));
            }
        }
        minStore.put(writeBatch);
    }
}
