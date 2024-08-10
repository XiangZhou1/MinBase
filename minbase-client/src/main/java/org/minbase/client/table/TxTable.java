package org.minbase.client.table;

import org.minbase.client.exception.ServerException;
import org.minbase.common.operation.ColumnValues;
import org.minbase.common.operation.Delete;
import org.minbase.common.operation.Get;
import org.minbase.common.operation.Put;
import org.minbase.common.rpc.proto.generated.ClientProto;
import org.minbase.common.rpc.proto.generated.TransactionServiceGrpc;
import org.minbase.common.table.Table;
import org.minbase.common.transaction.Transaction;
import org.minbase.common.utils.ByteUtil;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TxTable implements Table {
    private Transaction transaction;
    private String tableName;

    private TransactionServiceGrpc.TransactionServiceBlockingClient rpcClient;

    public TxTable(String tableName, TransactionServiceGrpc.TransactionServiceBlockingClient rpcClient) {
        this.tableName = tableName;
        this.rpcClient = rpcClient;
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public ColumnValues get(Get get) {
        final ClientProto.TxGetRequest.Builder builder = ClientProto.TxGetRequest.newBuilder();
        builder.setTxid(transaction.txId()).setTable(tableName).setKey(new String(get.getKey()));
        final List<byte[]> columns = get.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            builder.setColumns(i, new String(columns.get(i)));
        }
        final ClientProto.TxGetRequest getRequest = builder.build();
        final ClientProto.TxGetResponse getResponse = rpcClient.get(getRequest);
        ColumnValues value = new ColumnValues();
        for (int i = 0; i < getResponse.getColumnValuesCount(); i++) {
            final ClientProto.ColumnValue columnValues = getResponse.getColumnValues(i);
            value.set(ByteUtil.toBytes(columnValues.getColumn()), ByteUtil.toBytes(columnValues.getValue()));
        }
        return value;
    }

    @Override
    public void put(Put put) {
        final ClientProto.TxPutRequest.Builder builder = ClientProto.TxPutRequest.newBuilder();
        builder.setTxid(transaction.txId()).setTable(tableName).setKey(new String(put.getKey()));
        final TreeMap<byte[], byte[]> columnValues = put.getColumnValues();
        int i = 0;
        for (Map.Entry<byte[], byte[]> entry : columnValues.entrySet()) {
            ClientProto.ColumnValue.Builder columnValueBuilder = ClientProto.ColumnValue.newBuilder();
            columnValueBuilder.setColumn(new String(entry.getKey())).setValue(new String(entry.getValue()));
            builder.setColumnValues(i, columnValueBuilder.build());
        }
        final ClientProto.TxPutRequest putRequest = builder.build();
        final ClientProto.TxPutResponse putResponse = rpcClient.put(putRequest);

        final boolean success = putResponse.getSuccess();
        if (!success) {
            throw new ServerException("Put fail");
        }
    }

    @Override
    public boolean checkAndPut(byte[] checkKey, byte[] column, byte[] checkValue, Put put) {
        ClientProto.TxCheckAndPutRequest.Builder builder = ClientProto.TxCheckAndPutRequest.newBuilder();
        builder.setTable(tableName).setKey(new String(put.getKey()));
        builder.setCheckKey(new String(checkKey)).setCheckValue(new String(checkValue));
        TreeMap<byte[], byte[]> columnValues = put.getColumnValues();
        int i = 0;
        for (Map.Entry<byte[], byte[]> entry : columnValues.entrySet()) {
            ClientProto.ColumnValue.Builder columnValueBuilder = ClientProto.ColumnValue.newBuilder();
            columnValueBuilder.setColumn(new String(entry.getKey())).setValue(new String(entry.getValue()));
            builder.setColumnValues(i, columnValueBuilder.build());
        }

        ClientProto.TxCheckAndPutRequest checkAndPutRequest = builder.build();
        ClientProto.TxCheckAndPutResponse checkAndPutResponse = rpcClient.checkAndPut(checkAndPutRequest);

        return checkAndPutResponse.getSuccess();
    }

    @Override
    public void delete(Delete delete) {
        ClientProto.TxDeleteRequest.Builder builder = ClientProto.TxDeleteRequest.newBuilder();
        builder.setTable(tableName).setKey(new String(delete.getKey()));
        int i = 0;
        for (byte[] column : delete.getColumns()) {
            builder.setColumns(i++, new String(column));
        }

        ClientProto.TxDeleteRequest deleteRequest = builder.build();
        ClientProto.TxDeleteResponse deleteResponse = rpcClient.delete(deleteRequest);
    }
}
