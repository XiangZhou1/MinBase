package org.minbase.server.rpc;

import com.google.protobuf.ByteString;
import org.minbase.common.exception.TransactionException;
import org.minbase.common.exception.TransactionNotExistException;
import org.minbase.common.rpc.service.StatusCode;
import org.minbase.common.table.TableInfo;
import org.minbase.common.table.op.*;
import org.minbase.common.rpc.proto.generated.*;
import org.minbase.common.utils.ProtobufUtil;
import org.minbase.common.exception.TableNotExistException;
import org.minbase.server.table.TableManager;
import org.minbase.server.table.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class RpcService implements ClientServiceGrpc.ClientServiceBlockingClient,
        TransactionServiceGrpc.TransactionServiceBlockingClient, AdminServiceGrpc.AdminServiceBlockingClient {
    private static final Logger LOG = LoggerFactory.getLogger(RpcService.class);
    private TableManager tableManager;
    private List<Long> sessionTransactions = new ArrayList<>();

    public RpcService(TableManager tableManager) {
        this.tableManager = tableManager;
    }

    @Override
    public AdminProto.CreateTableResponse createTable(AdminProto.CreateTableRequest request) {
        AdminProto.CreateTableResponse.Builder builder = AdminProto.CreateTableResponse.newBuilder();
        try {
            boolean success = tableManager.createTable(request.getTableName().toStringUtf8());
            if (success) {
                builder.setStatusCode(StatusCode.SUCCESS.getCode());
            } else {
                builder.setStatusCode(StatusCode.FAIL.getCode());
            }
        } catch (Exception e) {
            LOG.error("Call createTable error, tableName:" + request.getTableName(), e);
            builder.setStatusCode(StatusCode.ERROR_DEFAULT.getCode());
        }
        return builder.build();
    }

    @Override
    public AdminProto.DropTableResponse dropTable(AdminProto.DropTableRequest request) {
        AdminProto.DropTableResponse.Builder builder = AdminProto.DropTableResponse.newBuilder();
        try {
            boolean success = tableManager.dropTable(request.getTableName().toStringUtf8());
            if (success) {
                builder.setStatusCode(StatusCode.SUCCESS.getCode());
            } else {
                builder.setStatusCode(StatusCode.FAIL.getCode());
            }
        } catch (Exception e) {
            LOG.error("Call dropTable error, tableName:" + request.getTableName(), e);
            builder.setStatusCode(StatusCode.ERROR_DEFAULT.getCode());
        }
        return builder.build();
    }

    @Override
    public AdminProto.TruncateTableResponse truncateTable(AdminProto.TruncateTableRequest request) {
        AdminProto.TruncateTableResponse.Builder builder = AdminProto.TruncateTableResponse.newBuilder();
        try {
            boolean success = tableManager.truncateTable(request.getTableName().toStringUtf8());
            if (success) {
                builder.setStatusCode(StatusCode.SUCCESS.getCode());
            } else {
                builder.setStatusCode(StatusCode.FAIL.getCode());
            }
        } catch (Exception e) {
            LOG.error("Call truncateTable error, tableName:" + request.getTableName(), e);
            builder.setStatusCode(StatusCode.ERROR_DEFAULT.getCode());
        }
        return builder.build();
    }

    @Override
    public ClientProto.GetResponse get(ClientProto.GetRequest request) {
        ClientProto.GetResponse.Builder builder = ClientProto.GetResponse.newBuilder();
        try {
            Get get = ProtobufUtil.toGet(request);
            ColumnValues columnValues = tableManager.get(request.getTable().toStringUtf8(), get);
            ProtobufUtil.toGetResponse(builder, request.getKey().toStringUtf8(), columnValues);
            builder.setStatusCode(StatusCode.SUCCESS.getCode());
        } catch (TableNotExistException e) {
            LOG.warn("Call get error, tableName:" + request.getTable() +", key:" + request.getKey(), e);
            builder.setStatusCode(StatusCode.ERROR_TABLE_NOT_EXIST.getCode());
            builder.setKey(ByteString.EMPTY);
        } catch (IOException e) {
            LOG.error("Call get error, tableName:" + request.getTable(), e);
            builder.setStatusCode(StatusCode.ERROR_IO_ERRER.getCode());
            builder.setKey(ByteString.EMPTY);
        } catch (Exception e) {
            LOG.error("Call get error, tableName:" + request.getTable(), e);
            builder.setStatusCode(StatusCode.ERROR_DEFAULT.getCode());
            builder.setKey(ByteString.EMPTY);
        }
        return builder.build();
    }

    @Override
    public ClientProto.PutResponse put(ClientProto.PutRequest request) {
        ClientProto.PutResponse.Builder builder = ClientProto.PutResponse.newBuilder();
        try {
            Put put = ProtobufUtil.toPut(request);
            tableManager.put(request.getTable().toStringUtf8(), put);
            builder.setStatusCode(StatusCode.SUCCESS.getCode());
        } catch (TableNotExistException e) {
            LOG.warn("Call put error, tableName:" + request.getTable() +", key:" + request.getKey(), e);
            builder.setStatusCode(StatusCode.ERROR_TABLE_NOT_EXIST.getCode());
        } catch (Exception e) {
            LOG.error("Call put error, tableName:" + request.getTable(), e);
            builder.setStatusCode(StatusCode.ERROR_DEFAULT.getCode());
        }
        return builder.build();
    }

    @Override
    public ClientProto.CheckAndPutResponse checkAndPut(ClientProto.CheckAndPutRequest request) {
        ClientProto.CheckAndPutResponse.Builder builder = ClientProto.CheckAndPutResponse.newBuilder();
        CheckAndPut checkAndPut = ProtobufUtil.toChecAndPut(request);
        try {
            boolean success = tableManager.checkAndPut(request.getTable().toStringUtf8(), checkAndPut);
            if (success) {
                builder.setStatusCode(StatusCode.SUCCESS.getCode());
            } else {
                builder.setStatusCode(StatusCode.FAIL.getCode());
            }
        } catch (TableNotExistException e) {
            LOG.warn("Call checkAndPut error, tableName:" + request.getTable() +", key:" + request.getKey(), e);
            builder.setStatusCode(StatusCode.ERROR_TABLE_NOT_EXIST.getCode());
        } catch (IOException e) {
            LOG.error("Call checkAndPut error, tableName:" + request.getTable(), e);
            builder.setStatusCode(StatusCode.ERROR_IO_ERRER.getCode());
        }  catch (Exception e) {
            LOG.error("Call checkAndPut error, tableName:" + request.getTable(), e);
            builder.setStatusCode(StatusCode.ERROR_DEFAULT.getCode());
        }
        return builder.build();
    }

    @Override
    public ClientProto.DeleteResponse delete(ClientProto.DeleteRequest request) {
        ClientProto.DeleteResponse.Builder builder = ClientProto.DeleteResponse.newBuilder();
        Delete delete = ProtobufUtil.toDelete(request);
        try {
            tableManager.delete(request.getTable().toStringUtf8(), delete);
            builder.setStatusCode(StatusCode.SUCCESS.getCode());
        } catch (TableNotExistException e) {
            LOG.warn("Call delete error, tableName:" + request.getTable() +", key:" + request.getKey(), e);
            builder.setStatusCode(StatusCode.ERROR_TABLE_NOT_EXIST.getCode());
        }  catch (Exception e) {
            LOG.error("Call delete error, tableName:" + request.getTable(), e);
            builder.setStatusCode(StatusCode.ERROR_DEFAULT.getCode());
        }
        return builder.build();
    }

    @Override
    public ClientProto.BeginTransactionResponse beginTransaction(ClientProto.BeginTransactionRequest request) {
        ClientProto.BeginTransactionResponse.Builder builder = ClientProto.BeginTransactionResponse.newBuilder();
        Transaction transaction = tableManager.newTransaction();
        if (transaction != null) {
            builder.setStatusCode(StatusCode.SUCCESS.getCode()).setTxid(transaction.getTxId());
            sessionTransactions.add(transaction.getTxId());
        } else {
            builder.setStatusCode(StatusCode.FAIL.getCode()).setTxid(0);
        }
        return builder.build();
    }

    @Override
    public ClientProto.RollBackResponse rollBack(ClientProto.RollBackRequest request) {
        ClientProto.RollBackResponse.Builder builder = ClientProto.RollBackResponse.newBuilder();
        tableManager.rollBackTransaction(request.getTxid());
        builder.setStatusCode(StatusCode.SUCCESS.getCode());
        sessionTransactions.remove(request.getTxid());
        return builder.build();
    }

    @Override
    public ClientProto.CommitResponse commit(ClientProto.CommitRequest request) {
        ClientProto.CommitResponse.Builder builder = ClientProto.CommitResponse.newBuilder();
        try {
            tableManager.commitTransaction(request.getTxid());
            builder.setStatusCode(StatusCode.SUCCESS.getCode());
            sessionTransactions.remove(request.getTxid());
        } catch (TransactionException e) {
            LOG.warn("Call commit error, txid:" + request.getTxid(), e);
            builder.setStatusCode(StatusCode.ERROR_TRANSACTION_CONFLICT.getCode());
        } catch (TransactionNotExistException e) {
            LOG.warn("Call commit error, txid:" + request.getTxid(), e);
            builder.setStatusCode(StatusCode.ERROR_TRANSACTION_NOT_EXIST.getCode());
        } catch (Exception e) {
            LOG.error("Call delete error, txid:" + request.getTxid(), e);
            builder.setStatusCode(StatusCode.ERROR_DEFAULT.getCode());
        }
        return builder.build();
    }

    @Override
    public ClientProto.TxGetResponse get(ClientProto.TxGetRequest request) {
        ClientProto.TxGetResponse.Builder builder = ClientProto.TxGetResponse.newBuilder();
        Get get = ProtobufUtil.toGet(request);
        try {
            ColumnValues columnValues = tableManager.txGet(request.getTxid(), request.getTable().toStringUtf8(), get);
            ProtobufUtil.toTxGetResponse(builder, request.getKey().toStringUtf8(), columnValues);
            builder.setStatusCode(StatusCode.SUCCESS.getCode());
        } catch (TransactionException e) {
            LOG.warn("Call txGet error, txid:" + request.getTxid() + ", table:" + request.getTable(), e);
            builder.setStatusCode(StatusCode.ERROR_TRANSACTION_CONFLICT.getCode());
            builder.setKey(ByteString.EMPTY);
        } catch (TransactionNotExistException e) {
            LOG.warn("Call txGet error, txid:" + request.getTxid() + ", table:" + request.getTable(), e);
            builder.setStatusCode(StatusCode.ERROR_TRANSACTION_NOT_EXIST.getCode());
            builder.setKey(ByteString.EMPTY);
        } catch (Exception e) {
            LOG.error("Call txGet error, tableName:" + request.getTxid() + ", table:" + request.getTable(), e);
            builder.setStatusCode(StatusCode.ERROR_DEFAULT.getCode());
            builder.setKey(ByteString.EMPTY);
        }
        return builder.build();
    }

    @Override
    public ClientProto.TxPutResponse put(ClientProto.TxPutRequest request) {
        ClientProto.TxPutResponse.Builder builder = ClientProto.TxPutResponse.newBuilder();
        Put put = ProtobufUtil.toPut(request);
        try {
            tableManager.txPut(request.getTxid(), request.getTable().toStringUtf8(), put);
            builder.setStatusCode(StatusCode.SUCCESS.getCode());
        } catch (TransactionException e) {
            LOG.warn("Call txGet error, txid:" + request.getTxid() + ", table:" + request.getTable(), e);
            builder.setStatusCode(StatusCode.ERROR_TRANSACTION_CONFLICT.getCode());
        } catch (TransactionNotExistException e) {
            LOG.warn("Call txGet error, txid:" + request.getTxid() + ", table:" + request.getTable(), e);
            builder.setStatusCode(StatusCode.ERROR_TRANSACTION_NOT_EXIST.getCode());
        } catch (Exception e) {
            LOG.error("Call txGet error, tableName:" + request.getTxid() + ", table:" + request.getTable(), e);
            builder.setStatusCode(StatusCode.ERROR_DEFAULT.getCode());
        }
        return builder.build();
    }

    @Override
    public ClientProto.TxCheckAndPutResponse checkAndPut(ClientProto.TxCheckAndPutRequest request) {
        ClientProto.TxCheckAndPutResponse.Builder builder = ClientProto.TxCheckAndPutResponse.newBuilder();
        CheckAndPut checkAndPut = ProtobufUtil.toTxCheckAndPut(request);
        try {
            boolean success = tableManager.txCheckAndPut(request.getTxid(), request.getTable().toStringUtf8(), checkAndPut);
            if (success) {
                builder.setStatusCode(StatusCode.SUCCESS.getCode());
            } else {
                builder.setStatusCode(StatusCode.FAIL.getCode());
            }
        } catch (TransactionException e) {
            LOG.warn("Call txGet error, txid:" + request.getTxid() + ", table:" + request.getTable(), e);
            builder.setStatusCode(StatusCode.ERROR_TRANSACTION_CONFLICT.getCode());
        } catch (TransactionNotExistException e) {
            LOG.warn("Call txGet error, txid:" + request.getTxid() + ", table:" + request.getTable(), e);
            builder.setStatusCode(StatusCode.ERROR_TRANSACTION_NOT_EXIST.getCode());
        } catch (Exception e) {
            LOG.error("Call txGet error, tableName:" + request.getTxid() + ", table:" + request.getTable(), e);
            builder.setStatusCode(StatusCode.ERROR_DEFAULT.getCode());
        }
        return builder.build();
    }

    @Override
    public ClientProto.TxDeleteResponse delete(ClientProto.TxDeleteRequest request) {
        ClientProto.TxDeleteResponse.Builder builder = ClientProto.TxDeleteResponse.newBuilder();
        Delete delete = ProtobufUtil.toDelete(request);
        try {
            tableManager.txDelete(request.getTxid(), request.getTable().toStringUtf8(), delete);
            builder.setStatusCode(StatusCode.SUCCESS.getCode());
        } catch (TransactionException e) {
            LOG.warn("Call txGet error, txid:" + request.getTxid() + ", table:" + request.getTable(), e);
            builder.setStatusCode(StatusCode.ERROR_TRANSACTION_CONFLICT.getCode());
        } catch (TransactionNotExistException e) {
            LOG.warn("Call txGet error, txid:" + request.getTxid() + ", table:" + request.getTable(), e);
            builder.setStatusCode(StatusCode.ERROR_TRANSACTION_NOT_EXIST.getCode());
        } catch (Exception e) {
            LOG.error("Call txGet error, tableName:" + request.getTxid() + ", table:" + request.getTable(), e);
            builder.setStatusCode(StatusCode.ERROR_DEFAULT.getCode());
        }
        return builder.build();
    }

    @Override
    public AdminProto.ListTablesResponse listTables(AdminProto.ListTablesRequest request) {
        AdminProto.ListTablesResponse.Builder builder = AdminProto.ListTablesResponse.newBuilder();
        List<TableInfo> tableInfos = tableManager.listTableNames();
        for (TableInfo tableInfo : tableInfos) {
            AdminProto.TableInfo.Builder builder1 = AdminProto.TableInfo.newBuilder();
            builder1.setName(ByteString.copyFromUtf8(tableInfo.getName()));
            Iterator<String> iterator = tableInfo.getColumns().iterator();
            while (iterator.hasNext()) {
                String next = iterator.next();
                builder1.addColumns(ByteString.copyFromUtf8(next));
            }
            builder.addTables(builder1.build());
        }
        return builder.build();
    }

    public void rollBackTransactions() {
        tableManager.rollBackTransactions(sessionTransactions);
    }
}
