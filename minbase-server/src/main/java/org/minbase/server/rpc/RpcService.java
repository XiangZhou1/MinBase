package org.minbase.server.rpc;

import org.minbase.common.table.op.*;
import org.minbase.common.rpc.proto.generated.*;
import org.minbase.common.table.Table;
import org.minbase.common.utils.ProtobufUtil;
import org.minbase.server.MinBaseServer;
import org.minbase.server.table.TableManager;
import org.minbase.server.table.Transaction;
import org.minbase.server.table.TransactionManager;
import org.minbase.server.table.TransactionState;

import java.util.ArrayList;
import java.util.List;

public class RpcService implements ClientServiceGrpc.ClientServiceBlockingClient, TransactionServiceGrpc.TransactionServiceBlockingClient, AdminServiceGrpc.AdminServiceBlockingClient {
    private TableManager tableManager;
    private List<Long> transactions = new ArrayList<>();

    public RpcService(MinBaseServer server) {
        this.tableManager = tableManager;
    }

    @Override
    public AdminProto.CreateTableResponse createTable(AdminProto.CreateTableRequest request) {
        AdminProto.CreateTableResponse.Builder builder = AdminProto.CreateTableResponse.newBuilder();
        try {
            boolean success = tableManager.createTable(request.getTableName());
            builder.setSuccess(success);
        } catch (Exception e) {
            e.printStackTrace();
            builder.setSuccess(false);
        }
        return builder.build();
    }

    @Override
    public AdminProto.DropTableResponse dropTable(AdminProto.DropTableRequest request) {
        return null;
    }

    @Override
    public AdminProto.TruncateTableResponse truncateTable(AdminProto.TruncateTableRequest request) {
        return null;
    }

    @Override
    public ClientProto.GetResponse get(ClientProto.GetRequest request) {
        Get get = ProtobufUtil.toGet(request);
        ColumnValues columnValues = tableManager.get(request.getTable(), get);
        return ProtobufUtil.toGetResponse(request.getKey(), columnValues);
    }

    @Override
    public ClientProto.PutResponse put(ClientProto.PutRequest request) {
        ClientProto.PutResponse.Builder builder = ClientProto.PutResponse.newBuilder();
        Put put = ProtobufUtil.toPut(request);
        boolean tableExist = tableManager.containTable(request.getTable());
        if (!tableExist) {
            builder.setSuccess(false);
            //throw new RuntimeException("Table not exist, table=" + request.getTable());
        }
        try {
            tableManager.put(request.getTable(), put);
            builder.setSuccess(true);
        } catch (Exception e) {
            e.printStackTrace();
            builder.setSuccess(false);
        }
        return builder.build();
    }

    @Override
    public ClientProto.CheckAndPutResponse checkAndPut(ClientProto.CheckAndPutRequest request) {
        ClientProto.CheckAndPutResponse.Builder builder = ClientProto.CheckAndPutResponse.newBuilder();
        CheckAndPut checkAndPut = ProtobufUtil.toChecAndPut(request);
        try {
            tableManager.checkAndPut(request.getTable(), checkAndPut);
            builder.setSuccess(true);
        } catch (Exception e) {
            e.printStackTrace();
            builder.setSuccess(false);
        }
        return builder.build();
    }

    @Override
    public ClientProto.DeleteResponse delete(ClientProto.DeleteRequest request) {
        ClientProto.DeleteResponse.Builder builder = ClientProto.DeleteResponse.newBuilder();
        Delete delete = ProtobufUtil.toDelete(request);
        try {
            tableManager.delete(request.getTable(), delete);
            builder.setSuccess(true);
        } catch (Exception e) {
            e.printStackTrace();
            builder.setSuccess(false);
        }
        return builder.build();
    }

    @Override
    public ClientProto.BeginTransactionResponse beginTransaction(ClientProto.BeginTransactionRequest request) {
        Transaction transaction = tableManager.newTransaction();
        ClientProto.BeginTransactionResponse.Builder builder = ClientProto.BeginTransactionResponse.newBuilder();
        if (transaction != null) {
            builder.setSuccess(true).setTxid(transaction.getTxId());
        } else {
            builder.setSuccess(false).setTxid(0);
        }
        return builder.build();
    }

    @Override
    public ClientProto.RollBackResponse rollBack(ClientProto.RollBackRequest request) {
        return null;
    }

    @Override
    public ClientProto.CommitResponse commit(ClientProto.CommitRequest request) {
        return null;
    }

    @Override
    public ClientProto.TxGetResponse get(ClientProto.TxGetRequest request) {
        ClientProto.TxGetResponse.Builder builder = ClientProto.TxGetResponse.newBuilder();
        Get get = ProtobufUtil.toGet(request);
        try {
            ColumnValues columnValues = tableManager.txGet(request.getTxid(), request.getTable(), get);
            return ProtobufUtil.toTxGetResponse(request.getKey(), columnValues);
        } catch (Exception e) {
            e.printStackTrace();
            //builder.setSuccess(false);
        }
        return builder.build();
    }

    @Override
    public ClientProto.TxPutResponse put(ClientProto.TxPutRequest request) {
        ClientProto.TxPutResponse.Builder builder = ClientProto.TxPutResponse.newBuilder();
        Put put = ProtobufUtil.toPut(request);
        try {
            tableManager.txPut(request.getTxid(), request.getTable(), put);
            builder.setSuccess(true);
        } catch (Exception e) {
            e.printStackTrace();
            builder.setSuccess(false);
        }
        return builder.build();
    }

    @Override
    public ClientProto.TxCheckAndPutResponse checkAndPut(ClientProto.TxCheckAndPutRequest request) {
        return null;
    }

    @Override
    public ClientProto.TxDeleteResponse delete(ClientProto.TxDeleteRequest request) {
        return null;
    }


    public void rollBackTransactions() {
//        tableManager.rollBackTransactions();
//        for (long txId : transactions) {
//            Transaction activeTransaction = TransactionManager.getActiveTransaction(txId);
//            activeTransaction.rollback();
//        }
    }
}
