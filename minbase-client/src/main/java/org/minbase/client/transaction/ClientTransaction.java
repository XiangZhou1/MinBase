package org.minbase.client.transaction;

import org.minbase.client.table.TxTableImpl;
import org.minbase.common.exception.ServerException;
import org.minbase.common.exception.TransactionException;
import org.minbase.common.exception.TransactionNotExistException;
import org.minbase.common.rpc.proto.generated.ClientProto;
import org.minbase.common.rpc.proto.generated.ClientServiceGrpc;
import org.minbase.common.rpc.proto.generated.TransactionServiceGrpc;
import org.minbase.common.rpc.service.StatusCode;
import org.minbase.common.table.TxTable;
import org.minbase.common.table.transaction.Transaction;

public class ClientTransaction implements Transaction {
    private long txId;
    private ClientServiceGrpc.ClientServiceBlockingClient rpcClient;
    private TransactionServiceGrpc.TransactionServiceBlockingClient txClient;

    public ClientTransaction(long txId, ClientServiceGrpc.ClientServiceBlockingClient rpcClient, TransactionServiceGrpc.TransactionServiceBlockingClient txClient) {
        this.txId = txId;
        this.rpcClient = rpcClient;
        this.txClient = txClient;
    }

    @Override
    public long txId() {
        return txId;
    }

    @Override
    public void commit() throws TransactionException, ServerException, TransactionNotExistException {
        ClientProto.CommitRequest.Builder builder = ClientProto.CommitRequest.newBuilder();
        ClientProto.CommitRequest commitRequest = builder.setTxid(txId).build();
        ClientProto.CommitResponse commitResponse = rpcClient.commit(commitRequest);
        if (commitResponse.getStatusCode() == StatusCode.ERROR_TRANSACTION_CONFLICT.getCode()) {
            throw new TransactionException();
        }
        if (commitResponse.getStatusCode() == StatusCode.ERROR_TRANSACTION_NOT_EXIST.getCode()) {
            throw new TransactionNotExistException();
        }
        if (commitResponse.getStatusCode() != StatusCode.SUCCESS.getCode()) {
            throw new ServerException("commit fail, txid:" + txClient);
        }
    }

    @Override
    public void rollback() throws TransactionException, ServerException, TransactionNotExistException {
        ClientProto.RollBackRequest.Builder builder = ClientProto.RollBackRequest.newBuilder();
        ClientProto.RollBackRequest rollBackRequest = builder.setTxid(txId).build();
        ClientProto.RollBackResponse rollBackResponse = rpcClient.rollBack(rollBackRequest);
        if (rollBackResponse.getStatusCode() == StatusCode.ERROR_TRANSACTION_CONFLICT.getCode()) {
            throw new TransactionException();
        }
        if (rollBackResponse.getStatusCode() == StatusCode.ERROR_TRANSACTION_NOT_EXIST.getCode()) {
            throw new TransactionNotExistException();
        }
        if (rollBackResponse.getStatusCode() != StatusCode.SUCCESS.getCode()) {
            throw new ServerException("commit fail, txid:" + txClient);
        }
    }

    @Override
    public TxTable getTable(String tableName) {
        return new TxTableImpl(tableName, this, txClient);
    }
}
