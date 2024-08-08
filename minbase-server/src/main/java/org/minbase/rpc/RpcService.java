package org.minbase.rpc;

import org.minbase.common.operation.ColumnValues;
import org.minbase.common.operation.Get;
import org.minbase.common.operation.Put;
import org.minbase.common.rpc.proto.generated.*;
import org.minbase.common.table.Table;
import org.minbase.common.utils.ProtobufUtil;
import org.minbase.server.MinBaseServer;
import org.minbase.server.transaction.Transaction;
import org.minbase.server.transaction.TransactionManager;
import org.minbase.server.transaction.TransactionState;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

public class RpcService implements ClientServiceGrpc.ClientServiceBlockingClient, TransactionServiceGrpc.TransactionServiceBlockingClient, AdminServiceGrpc.AdminServiceBlockingClient {
    private MinBaseServer server;
    private List<Long> transactions = new ArrayList<>();

    public RpcService(MinBaseServer server) {
        this.server = server;
    }

    @Override
    public AdminProto.CreateTableResponse createTable(AdminProto.CreateTableRequest request) {
        AdminProto.CreateTableResponse.Builder builder = AdminProto.CreateTableResponse.newBuilder();
        try {
            Table table = server.createTable(request.getTableName());
            builder.setSuccess(table != null);
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
        final Table table = server.getTable(request.getTable());
        if (table == null) {
            throw new RuntimeException("Table not exist, table=" + request.getTable());
        }
        final ColumnValues columnValues = table.get(get);
        return ProtobufUtil.toGetResponse(request.getKey(), columnValues);

    }

    @Override
    public ClientProto.PutResponse put(ClientProto.PutRequest request) {
        ClientProto.PutResponse.Builder builder = ClientProto.PutResponse.newBuilder();

        Put put = ProtobufUtil.toPut(request);
        Table table = server.getTable(request.getTable());
        if (table == null) {
            builder.setSuccess(false);
            //throw new RuntimeException("Table not exist, table=" + request.getTable());
        }
        try {
            table.put(put);
            builder.setSuccess(true);
        } catch (Exception e) {
            e.printStackTrace();
            builder.setSuccess(false);
        }
        return builder.build();
    }

    @Override
    public ClientProto.CheckAndPutResponse checkAndPut(ClientProto.CheckAndPutRequest request) {
        return null;
    }

    @Override
    public ClientProto.DeleteResponse delete(ClientProto.DeleteRequest request) {
        return null;
    }

    @Override
    public ClientProto.BeginTransactionResponse beginTransaction(ClientProto.BeginTransactionRequest request) {
        Transaction transaction = server.newTransaction();
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
        Transaction transaction = TransactionManager.getActiveTransaction(request.getTxid());
        if (transaction == null || !transaction.getTransactionState().equals(TransactionState.Active)) {
            //builder.set(false);
        } else {
            Table table = transaction.getTable(request.getTable());
            if (table == null) {
                //builder.setSuccess(false);
                //throw new RuntimeException("Table not exist, table=" + request.getTable());
            }
            try {
                ColumnValues columnValues = table.get(get);
                return ProtobufUtil.toTxGetResponse(request.getKey(), columnValues);

            } catch (Exception e) {
                e.printStackTrace();
                //builder.setSuccess(false);
            }
        }
        return builder.build();
    }

    @Override
    public ClientProto.TxPutResponse put(ClientProto.TxPutRequest request) {
        ClientProto.TxPutResponse.Builder builder = ClientProto.TxPutResponse.newBuilder();
        Put put = ProtobufUtil.toPut(request);
        Transaction transaction = TransactionManager.getActiveTransaction(request.getTxid());
        if (transaction == null || !transaction.getTransactionState().equals(TransactionState.Active)) {
            builder.setSuccess(false);
        } else {
            Table table = transaction.getTable(request.getTable());
            if (table == null) {
                builder.setSuccess(false);
                //throw new RuntimeException("Table not exist, table=" + request.getTable());
            }
            try {
                table.put(put);
                builder.setSuccess(true);
            } catch (Exception e) {
                e.printStackTrace();
                builder.setSuccess(false);
            }
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
        for (Long txid : transactions) {
            ConcurrentSkipListMap<Long, Transaction> activeTransactions = TransactionManager.getActiveTransactions();
            Transaction transaction = activeTransactions.get(txid);
            if (transaction != null && transaction.getTransactionState().equals(TransactionState.Active)) {
                transaction.rollback();
            }
        }
    }
}
