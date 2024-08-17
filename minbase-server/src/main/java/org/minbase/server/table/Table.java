package org.minbase.server.table;

import org.minbase.common.op.ColumnValues;
import org.minbase.common.op.Delete;
import org.minbase.common.op.Get;
import org.minbase.common.op.Put;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.conf.Config;
import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.minstore.MinStore;
import org.minbase.server.table.kv.InternalKey;
import org.minbase.server.table.kv.RowTacker;
import org.minbase.server.utils.ValueUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class Table implements org.minbase.common.table.Table {
    TableMeta tableMeta;
    private MinStore minStore;

    public Table(TableMeta tableMeta, MinStore minStore) {
        this.tableMeta = tableMeta;
        this.minStore = minStore;
    }

    public void setMinStore(MinStore minStore) {
        this.minStore = minStore;
    }

    public MinStore getMinStore() {
        return minStore;
    }

    @Override
    public String name() {
        return tableMeta.getName();
    }

    @Override
    public ColumnValues get(Get get) {
        ColumnValues columnValues = new org.minbase.server.table.kv.ColumnValues();
        if (get.getColumns().size() == 1 || tableMeta.getColumns().size() == 1) {
            KeyValue keyValue = minStore.get(new InternalKey(ByteUtil.toBytes(get.getKey()), ByteUtil.toBytes(get.getColumns().get(0)), get.getSequenceId()));

            org.minbase.server.kv.Value value = keyValue.getValue();
            if (value.isDelete()) {
                return columnValues;
            } else {
                InternalKey internalKey = (InternalKey) keyValue.getKey();
                columnValues.set(ByteUtil.toBytes(internalKey.getColumn()), value.getData());
            }
        } else {
            List<KeyValue> keyValues = scanColumns(get);
            for (KeyValue keyValue : keyValues) {
                InternalKey key = (InternalKey) keyValue.getKey();
                columnValues.set(ByteUtil.toBytes(key.getColumn()), keyValue.getValue().getData());
            }
        }
        return columnValues;
    }

    private List<KeyValue> scanColumns(Get get) {
        Key minKey;
        Key maxKey;
        List<String> columns;
        if (!get.getColumns().isEmpty()) {
            columns = get.getColumns();
            minKey = new InternalKey(ByteUtil.toBytes(get.getKey()), ByteUtil.toBytes(columns.get(0)), Long.MAX_VALUE);
            maxKey = new InternalKey(ByteUtil.toBytes(get.getKey()), ByteUtil.toBytes(columns.get(columns.size() - 1)), get.getSequenceId()+1);
        } else {
            columns = tableMeta.getColumns();
            minKey = new InternalKey(ByteUtil.toBytes(get.getKey()), ByteUtil.toBytes(columns.get(0)), Long.MAX_VALUE);
            maxKey = new InternalKey(ByteUtil.toBytes(get.getKey()), ByteUtil.toBytes(columns.get(columns.size() - 1)), get.getSequenceId()+1);
        }

        KeyValueIterator iterator = minStore.iterator(minKey, maxKey);
        try {
            RowTacker tacker = new RowTacker(get.getKey(), new HashSet<>(columns), get.getSequenceId());
            while (iterator.isValid()) {
                KeyValue tmp = iterator.value();
                tacker.track(tmp);
                if (tacker.shouldStop()) {
                    return tacker.getKeyValues();
                }
                iterator.nextInnerKey();
            }
            return tacker.getKeyValues();
        } finally {
            iterator.close();
        }
    }

    @Override
    public void put(Put put) {
        byte[] userKey = put.getKey();
        for (Map.Entry<byte[], byte[]> entry : put.getColumnValues().entrySet()) {
            byte[] column = entry.getKey();
            byte[] data = entry.getValue();
            minStore.put(new InternalKey(userKey, column, put.getSequenceId()), ValueUtils.Put(data));
        }
    }
    @Override
    public boolean checkAndPut(byte[] checkKey, byte[] column, byte[] checkValue, Put put) {
        minStore.writeLock();
        try {
            KeyValue keyValue = minStore.get(new InternalKey(checkKey, column, put.getSequenceId()));
            if (keyValue == null && checkValue != null) {
                return false;
            }
            byte[] data = keyValue.getValue().getData();
            if (checkValue != null || !ByteUtil.byteEqual(data, checkValue)) {
                return false;
            }
            put(put);
            return true;
        } finally {
            minStore.writeUnLock();
        }
    }

    @Override
    public void delete(Delete delete) {
        List<String> columns;
        if (!delete.getColumns().isEmpty()) {
            columns = delete.getColumns();
        } else {
            columns = tableMeta.getColumns();
        }

        for (String column : columns) {
            InternalKey key = new InternalKey(ByteUtil.toBytes(delete.getKey()), ByteUtil.toBytes(column), delete.getSequenceId());
            minStore.delete(key);
        }
    }

    public List<String> getColumns() {
        return tableMeta.getColumns();
    }

    public KeyValueIterator iterator(Key minKey, Key maxKey) {
        return minStore.iterator(minKey, maxKey);
    }

    public void updateTableMeta() throws IOException {
        File metaFile = new File(Config.DATA_DIR, name() + File.separator + ".tableMeta");
        try (FileOutputStream outputStream = new FileOutputStream(metaFile)) {
            tableMeta.encodeToFile(outputStream);
            outputStream.flush();
        }
    }

    public void addColumns(List<String> columns) throws IOException {
        tableMeta.getColumns().addAll(columns);
        updateTableMeta();
    }
}
