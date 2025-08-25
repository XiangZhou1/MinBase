package org.minbase.common.utils;

import com.google.protobuf.ByteString;
import org.minbase.common.rpc.proto.generated.AdminProto;
import org.minbase.common.table.Row;
import org.minbase.common.table.TableInfo;
import org.minbase.common.table.op.*;
import org.minbase.common.rpc.proto.generated.ClientProto;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ProtobufUtil {
    public static Get toGet(ClientProto.GetRequest request) {
        Get get = new Get(request.getKey().toByteArray());
        int columnsCount = request.getColumnsCount();
        for (int i = 0; i < columnsCount; i++) {
            final String column = request.getColumns(i).toStringUtf8();
            get.addColumn(ByteUtil.toBytes(column));
        }
        return get;
    }

    public static Get toGet(ClientProto.TxGetRequest request) {
        Get get = new Get(request.getKey().toByteArray());
        int columnsCount = request.getColumnsCount();
        for (int i = 0; i < columnsCount; i++) {
            final String column = request.getColumns(i).toStringUtf8();
            get.addColumn(ByteUtil.toBytes(column));
        }
        return get;
    }


    public static Put toPut(ClientProto.PutRequest request) {
        Put put = new Put(request.getKey().toByteArray());
        int count = request.getColumnValuesCount();
        for (int i = 0; i < count; i++) {
            ClientProto.ColumnValue columnValues = request.getColumnValues(i);
            put.addValue(columnValues.getColumn().toByteArray(), columnValues.getTableValue().toByteArray());
        }
        return put;
    }

    public static Put toPut(ClientProto.TxPutRequest request) {
        Put put = new Put(request.getKey().toByteArray());
        int count = request.getColumnValuesCount();
        for (int i = 0; i < count; i++) {
            ClientProto.ColumnValue columnValues = request.getColumnValues(i);
            put.addValue(columnValues.getColumn().toByteArray(), columnValues.getTableValue().toByteArray());
        }
        return put;
    }

    public static ClientProto.GetResponse.Builder toGetResponse(ClientProto.GetResponse.Builder builder, String key, ColumnValues columnValues) {
        builder.setKey(ByteString.copyFromUtf8(key));
        for (Map.Entry<byte[], byte[]> entry : columnValues.getColumnValues().entrySet()) {
            final ClientProto.ColumnValue.Builder builder1 = ClientProto.ColumnValue.newBuilder();
            builder1.setColumn(ByteString.copyFrom(entry.getKey())).setTableValue(ByteString.copyFrom(entry.getValue()));
            builder.addColumnValues(builder1.build());
        }
        return builder;
    }
    public static ClientProto.TxGetResponse.Builder toTxGetResponse(ClientProto.TxGetResponse.Builder builder, String key, ColumnValues columnValues) {
        builder.setKey(ByteString.copyFromUtf8(key));
        for (Map.Entry<byte[], byte[]> entry : columnValues.getColumnValues().entrySet()) {
            final ClientProto.ColumnValue.Builder builder1 = ClientProto.ColumnValue.newBuilder();
            builder1.setColumn(ByteString.copyFrom(entry.getKey())).setTableValue(ByteString.copyFrom(entry.getValue()));
            builder.addColumnValues(builder1.build());
        }
        return builder;
    }

    public static CheckAndPut toChecAndPut(ClientProto.CheckAndPutRequest request) {
        Put put = new Put(request.getKey().toByteArray());
        int count = request.getColumnValuesCount();
        for (int i = 0; i < count; i++) {
            ClientProto.ColumnValue columnValues = request.getColumnValues(i);
            put.addValue(columnValues.getColumn().toByteArray(), columnValues.getTableValue().toByteArray());
        }
        return new CheckAndPut(request.getCheckKey().toByteArray(),
                request.getCheckColumn().toByteArray(), request.getCheckValue().toByteArray(), put);
    }
    public static CheckAndPut toTxCheckAndPut(ClientProto.TxCheckAndPutRequest request) {
        Put put = new Put(request.getKey().toByteArray());
        int count = request.getColumnValuesCount();
        for (int i = 0; i < count; i++) {
            ClientProto.ColumnValue columnValues = request.getColumnValues(i);
            put.addValue(columnValues.getColumn().toByteArray(), columnValues.getTableValue().toByteArray());
        }
        return new CheckAndPut(request.getCheckKey().toByteArray(),
                request.getCheckColumn().toByteArray(), request.getCheckValue().toByteArray(), put);
    }

    public static Delete toDelete(ClientProto.DeleteRequest request) {
        List<byte[]> columns = new ArrayList<>();
        int columnsCount = request.getColumnsCount();
        for (int i = 0; i < columnsCount; i++) {
            columns.add(request.getColumns(i).toByteArray());
        }
        return new Delete(request.getKey().toByteArray(), columns);
    }

    public static Delete toDelete(ClientProto.TxDeleteRequest request) {
        List<byte[]> columns = new ArrayList<>();
        int columnsCount = request.getColumnsCount();
        for (int i = 0; i < columnsCount; i++) {
            columns.add(request.getColumns(i).toByteArray());
        }
        return new Delete(request.getKey().toByteArray(), columns);
    }

    public static List<TableInfo> toTableInfos(AdminProto.ListTablesResponse listTablesResponse) {
        List<TableInfo> tableInfos = new ArrayList<>();
        int tablesCount = listTablesResponse.getTablesCount();
        for (int i = 0; i < tablesCount; i++) {
            AdminProto.TableInfo tables = listTablesResponse.getTables(i);
            TableInfo tableInfo = new TableInfo(tables.getName().toStringUtf8());
            for (ByteString column : tables.getColumnsList()) {
                tableInfo.addColumn(column.toStringUtf8());
            }
            tableInfos.add(tableInfo);
        }
        return tableInfos;
    }

    public static List<Row> toScan(ClientProto.ScanResponse scanResponse) {
        List<Row> rows = new ArrayList<>();
        for (ClientProto.Row row : scanResponse.getRowsList()) {
            Row row1 = new Row(row.getRowKey().toStringUtf8());
            for (ClientProto.ColumnValue columnValue : row.getColumnValuesList()) {
                row1.add(columnValue.getColumn().toStringUtf8(), columnValue.getTableValue().toStringUtf8());
            }
            rows.add(row1);
        }
        return rows;
    }

    public static void toScanResponse(ClientProto.ScanResponse.Builder builder, List<Row> rows) {
        for (Row row : rows) {
            ClientProto.Row.Builder rowBuilder = ClientProto.Row.newBuilder();
            rowBuilder.setRowKey(ByteString.copyFromUtf8(row.getRowKey()));
            ClientProto.ColumnValue.Builder columnValueBuilder = ClientProto.ColumnValue.newBuilder();
            for (Map.Entry<byte[], byte[]> entry : row.getColumnValues().getColumnValues().entrySet()) {
                columnValueBuilder.setColumn(ByteString.copyFrom(entry.getKey()))
                        .setTableValue(ByteString.copyFrom(entry.getValue()));
                rowBuilder.addColumnValues(columnValueBuilder.build());
            }
            builder.addRows(rowBuilder.build());
        }
    }
}
