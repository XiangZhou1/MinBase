package org.minbase.client.table;

import com.google.protobuf.ByteString;
import org.minbase.common.exception.ServerException;
import org.minbase.common.exception.TableNotExistException;
import org.minbase.common.exception.TransactionNotExistException;
import org.minbase.common.rpc.service.StatusCode;
import org.minbase.common.table.TxTable;
import org.minbase.common.table.op.ColumnValues;
import org.minbase.common.table.op.Delete;
import org.minbase.common.table.op.Get;
import org.minbase.common.table.op.Put;
import org.minbase.common.rpc.proto.generated.ClientProto;
import org.minbase.common.rpc.proto.generated.TransactionServiceGrpc;
import org.minbase.common.table.ClientTable;
import org.minbase.common.table.transaction.Transaction;
import org.minbase.common.utils.ByteUtil;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TxTableImpl implements TxTable {
    private Transaction transaction;
    private String tableName;

    private TransactionServiceGrpc.TransactionServiceBlockingClient rpcClient;

    public TxTableImpl(String tableName, Transaction transaction,
                       TransactionServiceGrpc.TransactionServiceBlockingClient rpcClient) {
        this.tableName = tableName;
        this.rpcClient = rpcClient;
        this.transaction = transaction;
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public ColumnValues get(Get get) throws TableNotExistException, ServerException, TransactionNotExistException {
        final ClientProto.TxGetRequest.Builder builder = ClientProto.TxGetRequest.newBuilder();
        builder.setTxid(transaction.txId()).setTable(ByteString.copyFromUtf8(tableName)).setKey(ByteString.copyFrom(get.getKey()));
        final List<byte[]> columns = get.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            builder.addColumns(ByteString.copyFrom(columns.get(i)));
        }
        final ClientProto.TxGetRequest getRequest = builder.build();
        final ClientProto.TxGetResponse getResponse = rpcClient.get(getRequest);
        if (getResponse.getStatusCode() == StatusCode.ERROR_TABLE_NOT_EXIST.getCode()) {
            throw new TableNotExistException(tableName + " not exist");
        }
        if (getResponse.getStatusCode() == StatusCode.ERROR_TRANSACTION_NOT_EXIST.getCode()) {
            throw new TransactionNotExistException(transaction.txId() + " not exist");
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
    public void put(Put put) throws TableNotExistException, TransactionNotExistException, ServerException {
        final ClientProto.TxPutRequest.Builder builder = ClientProto.TxPutRequest.newBuilder();
        builder.setTxid(transaction.txId()).setTable(ByteString.copyFromUtf8(tableName)).setKey(ByteString.copyFrom(put.getKey()));
        final TreeMap<byte[], byte[]> columnValues = put.getColumnValues();
        for (Map.Entry<byte[], byte[]> entry : columnValues.entrySet()) {
            ClientProto.ColumnValue.Builder columnValueBuilder = ClientProto.ColumnValue.newBuilder();
            columnValueBuilder.setColumn(ByteString.copyFrom(entry.getKey())).setTableValue(ByteString.copyFrom(entry.getValue()));
            builder.addColumnValues(columnValueBuilder.build());
        }
        final ClientProto.TxPutRequest putRequest = builder.build();
        final ClientProto.TxPutResponse putResponse = rpcClient.put(putRequest);

        if (putResponse.getStatusCode() == StatusCode.ERROR_TABLE_NOT_EXIST.getCode()) {
            throw new TableNotExistException(tableName + " not exist");
        }
        if (putResponse.getStatusCode() == StatusCode.ERROR_TRANSACTION_NOT_EXIST.getCode()) {
            throw new TransactionNotExistException(transaction.txId() + " not exist");
        }
        if (putResponse.getStatusCode() != StatusCode.SUCCESS.getCode()) {
            throw new ServerException("get " + new String(put.getKey()) + " fail");
        }
    }

    @Override
    public boolean checkAndPut(byte[] checkKey, byte[] checkColumn, byte[] checkValue, Put put)
            throws TableNotExistException, ServerException, TransactionNotExistException {
        ClientProto.TxCheckAndPutRequest.Builder builder = ClientProto.TxCheckAndPutRequest.newBuilder();
        builder.setTxid(transaction.txId()).setTable(ByteString.copyFromUtf8(tableName)).setKey(ByteString.copyFrom(put.getKey()));
        builder.setCheckKey(ByteString.copyFrom(checkKey)).setCheckValue(ByteString.copyFrom(checkValue));
        builder.setCheckColumn(ByteString.copyFrom(checkColumn));
        TreeMap<byte[], byte[]> columnValues = put.getColumnValues();
        for (Map.Entry<byte[], byte[]> entry : columnValues.entrySet()) {
            ClientProto.ColumnValue.Builder columnValueBuilder = ClientProto.ColumnValue.newBuilder();
            columnValueBuilder.setColumn(ByteString.copyFrom(entry.getKey())).setTableValue(ByteString.copyFrom(entry.getValue()));
            builder.addColumnValues(columnValueBuilder.build());
        }

        ClientProto.TxCheckAndPutRequest checkAndPutRequest = builder.build();
        ClientProto.TxCheckAndPutResponse checkAndPutResponse = rpcClient.checkAndPut(checkAndPutRequest);
        int statusCode = checkAndPutResponse.getStatusCode();
        if (statusCode == StatusCode.SUCCESS.getCode() || statusCode == StatusCode.FAIL.getCode()) {
            return statusCode == StatusCode.SUCCESS.getCode();
        }
        if (statusCode == StatusCode.ERROR_TRANSACTION_NOT_EXIST.getCode()) {
            throw new TransactionNotExistException(transaction.txId() + " not exist");
        }
        if (statusCode == StatusCode.ERROR_TABLE_NOT_EXIST.getCode()) {
            throw new TableNotExistException(tableName + "not exist");
        }
        throw new ServerException("checkAndPut " + new String(checkKey) + " fail");
    }

    @Override
    public void delete(Delete delete) throws TableNotExistException, TransactionNotExistException, ServerException {
        ClientProto.TxDeleteRequest.Builder builder = ClientProto.TxDeleteRequest.newBuilder();
        builder.setTxid(transaction.txId()).setTable(ByteString.copyFromUtf8(tableName)).setKey(ByteString.copyFrom(delete.getKey()));
        for (byte[] column : delete.getColumns()) {
            builder.addColumns(ByteString.copyFrom(column));
        }
        ClientProto.TxDeleteRequest deleteRequest = builder.build();
        ClientProto.TxDeleteResponse deleteResponse = rpcClient.delete(deleteRequest);
        if (deleteResponse.getStatusCode() == StatusCode.ERROR_TABLE_NOT_EXIST.getCode()) {
            throw new TableNotExistException(tableName + " not exist");
        }
        if (deleteResponse.getStatusCode() == StatusCode.ERROR_TRANSACTION_NOT_EXIST.getCode()) {
            throw new TransactionNotExistException(transaction.txId() + " not exist");
        }
        if (deleteResponse.getStatusCode() != StatusCode.SUCCESS.getCode()) {
            throw new ServerException("get " + new String(delete.getKey()) + " fail");
        }
    }
}
