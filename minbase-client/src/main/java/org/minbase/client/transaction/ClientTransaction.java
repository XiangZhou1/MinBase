package org.minbase.client.transaction;

import org.minbase.client.table.TxTable;
import org.minbase.common.exception.TransactionException;
import org.minbase.common.rpc.proto.generated.ClientProto;
import org.minbase.common.rpc.proto.generated.ClientServiceGrpc;
import org.minbase.common.rpc.proto.generated.TransactionServiceGrpc;
import org.minbase.common.table.Table;
import org.minbase.common.transaction.Transaction;

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
    public void commit() throws TransactionException {
        ClientProto.CommitRequest.Builder builder = ClientProto.CommitRequest.newBuilder();
        ClientProto.CommitRequest commitRequest = builder.setTxid(txId).build();
        ClientProto.CommitResponse commitResponse = rpcClient.commit(commitRequest);
        if(!commitResponse.getSuccess()){
            throw new TransactionException();
        }
    }

    @Override
    public void rollback() {
        ClientProto.RollBackRequest.Builder builder = ClientProto.RollBackRequest.newBuilder();
        ClientProto.RollBackRequest rollBackRequest = builder.setTxid(txId).build();
        ClientProto.RollBackResponse rollBackResponse = rpcClient.rollBack(rollBackRequest);
        if(!rollBackResponse.getSuccess()){
            throw new TransactionException();
        }
    }

    @Override
    public Table getTable(String tableName) {
        return new TxTable(tableName, txClient);
    }
}
