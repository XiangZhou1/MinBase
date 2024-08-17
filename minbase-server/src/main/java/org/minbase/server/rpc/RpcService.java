package org.minbase.server.rpc;

import org.minbase.common.op.ColumnValues;
import org.minbase.common.op.Delete;
import org.minbase.common.op.Get;
import org.minbase.common.op.Put;
import org.minbase.common.rpc.proto.generated.*;
import org.minbase.common.table.Table;
import org.minbase.common.utils.ByteUtil;
import org.minbase.common.utils.ProtobufUtil;
import org.minbase.server.MinBaseServer;
import org.minbase.server.transaction.Transaction;
import org.minbase.server.transaction.TransactionManager;
import org.minbase.server.transaction.TransactionState;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RpcService implements ClientServiceGrpc.ClientServiceBlockingClient, TransactionServiceGrpc.TransactionServiceBlockingClient, AdminServiceGrpc.AdminServiceBlockingClient {
    private MinBaseServer server;
    private List<Long> transactions = new ArrayList<>();

    public RpcService(MinBaseServer server) {
        this.server = server;
    }



    @Override
    public ClientProto.GetResponse get(ClientProto.GetRequest request) {
        Get get = ProtobufUtil.toGet(request);
        get.setSequenceId(TransactionManager.newTransactionId());
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
        put.setSequenceId(TransactionManager.newTransactionId());
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
        String checkKey = request.getCheckKey();
        String checkColumn = request.getCheckColumn();
        String checkValue = request.getCheckValue();
        Put put = new Put(ByteUtil.toBytes(request.getKey()));
        int columnValuesCount = request.getColumnValuesCount();
        for (int i = 0; i < columnValuesCount; i++) {
            ClientProto.ColumnValue columnValue = request.getColumnValues(i);
            put.addValue(ByteUtil.toBytes(columnValue.getColumn()), ByteUtil.toBytes(columnValue.getValue()));
        }

        ClientProto.CheckAndPutResponse.Builder builder = ClientProto.CheckAndPutResponse.newBuilder();
        put.setSequenceId(TransactionManager.newTransactionId());
        Table table = server.getTable(request.getTable());
        if (table == null) {
            builder.setSuccess(false);
            //throw new RuntimeException("Table not exist, table=" + request.getTable());
        }
        try {
            boolean success = table.checkAndPut(checkKey.getBytes(StandardCharsets.UTF_8), checkColumn.getBytes(StandardCharsets.UTF_8), checkValue.getBytes(StandardCharsets.UTF_8), put);
            builder.setSuccess(success);
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
        delete.setSequenceId(TransactionManager.newTransactionId());
        Table table = server.getTable(request.getTable());
        if (table == null) {
            builder.setSuccess(false);
            //throw new RuntimeException("Table not exist, table=" + request.getTable());
        }
        try {
            table.delete(delete);
            builder.setSuccess(true);
        } catch (Exception e) {
            e.printStackTrace();
            builder.setSuccess(false);
        }
        return builder.build();
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
        ClientProto.RollBackResponse.Builder builder = ClientProto.RollBackResponse.newBuilder();
        Transaction transaction = TransactionManager.getActiveTransaction(request.getTxid());
        if (transaction == null) {
            builder.setSuccess(true);
        } else if (transaction.getTransactionState().equals(TransactionState.Commit)) {
            builder.setSuccess(false);
        } else {
            transaction.rollback();
            transactions.remove(request.getTxid());
            builder.setSuccess(true);
        }
        return builder.build();
    }

    @Override
    public ClientProto.CommitResponse commit(ClientProto.CommitRequest request) {
        ClientProto.CommitResponse.Builder builder = ClientProto.CommitResponse.newBuilder();
        Transaction transaction = TransactionManager.getActiveTransaction(request.getTxid());
        if (transaction == null) {
            builder.setSuccess(false);
        } else if (!transaction.getTransactionState().equals(TransactionState.Active)) {
            builder.setSuccess(false);
        } else {
            transaction.commit();
            transactions.remove(request.getTxid());
            builder.setSuccess(true);
        }
        return builder.build();
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
        String checkKey = request.getCheckKey();
        String checkColumn = request.getCheckColumn();
        String checkValue = request.getCheckValue();
        Put put = new Put(ByteUtil.toBytes(request.getKey()));
        int columnValuesCount = request.getColumnValuesCount();
        for (int i = 0; i < columnValuesCount; i++) {
            ClientProto.ColumnValue columnValue = request.getColumnValues(i);
            put.addValue(ByteUtil.toBytes(columnValue.getColumn()), ByteUtil.toBytes(columnValue.getValue()));
        }

        ClientProto.TxCheckAndPutResponse.Builder builder = ClientProto.TxCheckAndPutResponse.newBuilder();
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
                boolean success = table.checkAndPut(checkKey.getBytes(StandardCharsets.UTF_8), checkColumn.getBytes(StandardCharsets.UTF_8), checkValue.getBytes(StandardCharsets.UTF_8), put);
                builder.setSuccess(success);
            } catch (Exception e) {
                e.printStackTrace();
                builder.setSuccess(false);
            }
        }
        return builder.build();
    }

    @Override
    public ClientProto.TxDeleteResponse delete(ClientProto.TxDeleteRequest request) {
        Delete delete = ProtobufUtil.toDelete(request);
        ClientProto.TxDeleteResponse.Builder builder = ClientProto.TxDeleteResponse.newBuilder();
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
                table.delete(delete);
                builder.setSuccess(true);
            } catch (Exception e) {
                e.printStackTrace();
                builder.setSuccess(false);
            }
        }
        return builder.build();
    }


    @Override
    public AdminProto.AddColumnResponse addColumn(AdminProto.AddColumnRequest request) {
        AdminProto.AddColumnResponse.Builder builder = AdminProto.AddColumnResponse.newBuilder();
        int columnsCount = request.getColumnsCount();
        List<String> columns = new ArrayList<>(columnsCount);
        for (int i = 0; i < columnsCount; i++) {
            columns.add(request.getColumns(i));
        }
        try {
            boolean success = server.addColumns(request.getTableName(), columns);
            builder.setSuccess(success);
        } catch (Exception e) {
            e.printStackTrace();
            builder.setSuccess(false);
        }
        return builder.build();
    }

    @Override
    public AdminProto.GetTableInfoResponse getTableInfo(AdminProto.GetTableInfoRequest request) {
        AdminProto.GetTableInfoResponse.Builder builder = AdminProto.GetTableInfoResponse.newBuilder();
        try {
            List<String> columns = server.getTable(request.getTableName()).getColumns();
            int i = 0;
            for (String column : columns) {
                builder.setColumns(i++, column);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return builder.build();
    }

    @Override
    public AdminProto.CreateTableResponse createTable(AdminProto.CreateTableRequest request) {
        AdminProto.CreateTableResponse.Builder builder = AdminProto.CreateTableResponse.newBuilder();
        int columnsCount = request.getColumnsCount();
        List<String> columns = new ArrayList<>(columnsCount);
        for (int i = 0; i < columnsCount; i++) {
            columns.add(request.getColumns(i));
        }
        try {
            Table table = server.createTable(request.getTableName(), columns);
            builder.setSuccess(table != null);
        } catch (Exception e) {
            e.printStackTrace();
            builder.setSuccess(false);
        }
        return builder.build();
    }

    @Override
    public AdminProto.DropTableResponse dropTable(AdminProto.DropTableRequest request) {
        AdminProto.DropTableResponse.Builder builder = AdminProto.DropTableResponse.newBuilder();
        try {
            boolean success = server.dropTable(request.getTableName());
            builder.setSuccess(success);
        } catch (Exception e) {
            e.printStackTrace();
            builder.setSuccess(false);
        }
        return builder.build();
    }

    @Override
    public AdminProto.TruncateTableResponse truncateTable(AdminProto.TruncateTableRequest request) {
        AdminProto.TruncateTableResponse.Builder builder = AdminProto.TruncateTableResponse.newBuilder();
        try {
            boolean success = server.truncateTable(request.getTableName());
            builder.setSuccess(success);
        } catch (Exception e) {
            e.printStackTrace();
            builder.setSuccess(false);
        }
        return builder.build();
    }

    public void rollBackTransactions() {
        for (long txId : transactions) {
            Transaction activeTransaction = TransactionManager.getActiveTransaction(txId);
            activeTransaction.rollback();
        }
    }

}
