package org.minbase.client.table;

import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.InvalidProtocolBufferException;
import org.minbase.common.exception.ServerException;
import org.minbase.common.exception.TableNotExistException;
import org.minbase.common.rpc.proto.generated.ClientProto;
import org.minbase.common.rpc.proto.generated.ClientServiceGrpc;
import org.minbase.common.rpc.service.StatusCode;
import org.minbase.common.table.ClientTable;
import org.minbase.common.table.op.ColumnValues;
import org.minbase.common.table.op.Delete;
import org.minbase.common.table.op.Get;
import org.minbase.common.table.op.Put;
import org.minbase.common.utils.ByteUtil;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ClientTableImpl implements ClientTable {
    private String tableName;
    private ClientServiceGrpc.ClientServiceBlockingClient rpcClient;


    public ClientTableImpl(String tableName, ClientServiceGrpc.ClientServiceBlockingClient rpcClient) {
        this.tableName = tableName;
        this.rpcClient = rpcClient;
    }

    @Override
    public String name() {
        return tableName;
    }

    @Override
    public ColumnValues get(Get get) throws TableNotExistException, ServerException {
        final ClientProto.GetRequest.Builder builder = ClientProto.GetRequest.newBuilder();
        builder.setTable(ByteString.copyFromUtf8(tableName)).setKey(ByteString.copyFrom(get.getKey()));
        final List<byte[]> columns = get.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            builder.addColumns(ByteString.copyFrom(columns.get(i)));
        }
        final ClientProto.GetRequest getRequest = builder.build();
        final ClientProto.GetResponse getResponse = rpcClient.get(getRequest);
        if (getResponse.getStatusCode() == StatusCode.ERROR_TABLE_NOT_EXIST.getCode()) {
            throw new TableNotExistException(tableName + "not exist");
        }
        if (getResponse.getStatusCode() != StatusCode.SUCCESS.getCode()) {
            throw new ServerException("get " + new String(get.getKey()) + " fail");
        }
        ColumnValues value = new ColumnValues();
        for (int i = 0; i < getResponse.getColumnValuesCount(); i++) {
            final ClientProto.ColumnValue columnValues = getResponse.getColumnValues(i);
            value.set(columnValues.getColumn().toByteArray(), columnValues.getTableValue().toByteArray());
        }
        return value;
    }

    @Override
    public void put(Put put) throws TableNotExistException, ServerException {
        final ClientProto.PutRequest.Builder builder = ClientProto.PutRequest.newBuilder();
        builder.setTable(ByteString.copyFromUtf8(tableName)).setKey(ByteString.copyFrom(put.getKey()));
        final TreeMap<byte[], byte[]> columnValues = put.getColumnValues();
        for (Map.Entry<byte[], byte[]> entry : columnValues.entrySet()) {
            ClientProto.ColumnValue.Builder columnValueBuilder = ClientProto.ColumnValue.newBuilder();
            columnValueBuilder.setColumn(ByteString.copyFrom(entry.getKey())).setTableValue(ByteString.copyFrom(entry.getValue()));
            builder.addColumnValues(columnValueBuilder.build());
        }
        final ClientProto.PutRequest putRequest = builder.build();
        final ClientProto.PutResponse putResponse = rpcClient.put(putRequest);
        if (putResponse.getStatusCode() == StatusCode.ERROR_TABLE_NOT_EXIST.getCode()) {
            throw new TableNotExistException(tableName + "not exist");
        }
        if (putResponse.getStatusCode() != StatusCode.SUCCESS.getCode()) {
            throw new ServerException("put " + new String(put.getKey()) + " fail");
        }
    }

    @Override
    public boolean checkAndPut(byte[] checkKey, byte[] checColumn, byte[] checkValue, Put put)
            throws TableNotExistException, ServerException {
            ClientProto.CheckAndPutRequest.Builder builder = ClientProto.CheckAndPutRequest.newBuilder();
            builder.setTable(ByteString.copyFromUtf8(tableName)).setKey(ByteString.copyFrom(put.getKey()));
            builder.setCheckKey(ByteString.copyFrom(checkKey)).setCheckValue(ByteString.copyFrom(checkValue));
            builder.setCheckColumn(ByteString.copyFrom(checColumn));
            TreeMap<byte[], byte[]> columnValues = put.getColumnValues();
            for (Map.Entry<byte[], byte[]> entry : columnValues.entrySet()) {
                ClientProto.ColumnValue.Builder columnValueBuilder = ClientProto.ColumnValue.newBuilder();
                columnValueBuilder.setColumn(ByteString.copyFrom(entry.getKey())).setTableValue(ByteString.copyFrom(entry.getValue()));
                builder.addColumnValues(columnValueBuilder.build());
            }

            ClientProto.CheckAndPutRequest checkAndPutRequest = builder.build();
            ClientProto.CheckAndPutResponse checkAndPutResponse = rpcClient.checkAndPut(checkAndPutRequest);
            int statusCode = checkAndPutResponse.getStatusCode();
            if (statusCode == StatusCode.SUCCESS.getCode() || statusCode == StatusCode.FAIL.getCode()) {
                return statusCode == StatusCode.SUCCESS.getCode();
            }
            if (statusCode == StatusCode.ERROR_TABLE_NOT_EXIST.getCode()) {
                throw new TableNotExistException(tableName + "not exist");
            }
            throw new ServerException("checkAndPut " + new String(checkKey) + " fail");


    }

    @Override
    public void delete(Delete delete) throws TableNotExistException, ServerException {
        ClientProto.DeleteRequest.Builder builder = ClientProto.DeleteRequest.newBuilder();
        builder.setTable(ByteString.copyFromUtf8(tableName)).setKey(ByteString.copyFrom(delete.getKey()));
        for (byte[] column : delete.getColumns()) {
            builder.addColumns(ByteString.copyFrom(column));
        }

        ClientProto.DeleteRequest deleteRequest = builder.build();
        ClientProto.DeleteResponse deleteResponse = rpcClient.delete(deleteRequest);
        int statusCode = deleteResponse.getStatusCode();
        if (statusCode == StatusCode.ERROR_TABLE_NOT_EXIST.getCode()) {
            throw new TableNotExistException(tableName + "not exist");
        }
        if (deleteResponse.getStatusCode() != StatusCode.SUCCESS.getCode()) {
            throw new ServerException("get " + new String(delete.getKey()) + " fail");
        }
    }
}
