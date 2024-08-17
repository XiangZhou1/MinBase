package org.minbase.client.table;

import org.minbase.client.exception.ServerException;
import org.minbase.common.op.Delete;
import org.minbase.common.op.Get;
import org.minbase.common.op.Put;
import org.minbase.common.op.ColumnValues;
import org.minbase.common.rpc.proto.generated.ClientProto;
import org.minbase.common.rpc.proto.generated.ClientServiceGrpc;
import org.minbase.common.table.*;
import org.minbase.common.utils.ByteUtil;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ClientTable implements Table {
    private String tableName;
    private ClientServiceGrpc.ClientServiceBlockingClient rpcClient;


    public ClientTable(String tableName, ClientServiceGrpc.ClientServiceBlockingClient rpcClient) {
        this.tableName = tableName;
        this.rpcClient = rpcClient;
    }

    @Override
    public String name() {
        return tableName;
    }

    @Override
    public ColumnValues get(Get get) {
        final ClientProto.GetRequest.Builder builder = ClientProto.GetRequest.newBuilder();
        builder.setTable(tableName).setKey(get.getKey());
        final List<String> columns = get.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            builder.setColumns(i, columns.get(i));
        }
        final ClientProto.GetRequest getRequest = builder.build();
        final ClientProto.GetResponse getResponse = rpcClient.get(getRequest);
        ColumnValues value = new ColumnValues();
        for (int i = 0; i < getResponse.getColumnValuesCount(); i++) {
            final ClientProto.ColumnValue columnValues = getResponse.getColumnValues(i);
            value.set(ByteUtil.toBytes(columnValues.getColumn()), ByteUtil.toBytes(columnValues.getValue()));
        }
        return value;
    }

    @Override
    public void put(Put put) {
        final ClientProto.PutRequest.Builder builder = ClientProto.PutRequest.newBuilder();
        builder.setTable(tableName).setKey(new String(put.getKey()));
        final TreeMap<byte[], byte[]> columnValues = put.getColumnValues();
        int i = 0;
        for (Map.Entry<byte[], byte[]> entry : columnValues.entrySet()) {
            ClientProto.ColumnValue.Builder columnValueBuilder = ClientProto.ColumnValue.newBuilder();
            columnValueBuilder.setColumn(new String(entry.getKey())).setValue(new String(entry.getValue()));
            builder.setColumnValues(i, columnValueBuilder.build());
        }
        final ClientProto.PutRequest putRequest = builder.build();
        final ClientProto.PutResponse putResponse = rpcClient.put(putRequest);

        final boolean success = putResponse.getSuccess();
        if (!success) {
            throw new ServerException("Put fail");
        }
    }

    @Override
    public boolean checkAndPut(byte[] checkKey, byte[] checkColumn, byte[] checkValue, Put put) {
        ClientProto.CheckAndPutRequest.Builder builder = ClientProto.CheckAndPutRequest.newBuilder();
        builder.setTable(tableName).setKey(new String(put.getKey()));
        builder.setCheckKey(new String(checkKey)).setCheckColumn(new String(checkColumn)).setCheckValue(new String(checkValue));
        TreeMap<byte[], byte[]> columnValues = put.getColumnValues();
        int i = 0;
        for (Map.Entry<byte[], byte[]> entry : columnValues.entrySet()) {
            ClientProto.ColumnValue.Builder columnValueBuilder = ClientProto.ColumnValue.newBuilder();
            columnValueBuilder.setColumn(new String(entry.getKey())).setValue(new String(entry.getValue()));
            builder.setColumnValues(i, columnValueBuilder.build());
        }

        ClientProto.CheckAndPutRequest checkAndPutRequest = builder.build();
        ClientProto.CheckAndPutResponse checkAndPutResponse = rpcClient.checkAndPut(checkAndPutRequest);

        return checkAndPutResponse.getSuccess();
    }

    @Override
    public void delete(Delete delete) {
        ClientProto.DeleteRequest.Builder builder = ClientProto.DeleteRequest.newBuilder();
        builder.setTable(tableName).setKey(delete.getKey());
        int i = 0;
        for (String column : delete.getColumns()) {
            builder.setColumns(i++, column);
        }

        ClientProto.DeleteRequest deleteRequest = builder.build();
        ClientProto.DeleteResponse deleteResponse = rpcClient.delete(deleteRequest);
    }
}
