package org.minbase.client.service;

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Promise;
import org.minbase.common.rpc.proto.generated.AdminProto;
import org.minbase.common.rpc.proto.generated.AdminServiceGrpc;
import org.minbase.common.rpc.proto.generated.RpcProto;
import org.minbase.common.rpc.service.CallType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class AdminService extends Service implements AdminServiceGrpc.AdminServiceBlockingClient {
    private static final Logger LOG = LoggerFactory.getLogger(AdminService.class);
    public AdminService(Channel channel, AtomicLong requestId, ConcurrentHashMap<Long, Promise<RpcProto.RpcResponse>> waitingResponses, EventLoopGroup group) {
        super(channel, requestId, waitingResponses, group);
    }

    @Override
    public AdminProto.CreateTableResponse createTable(AdminProto.CreateTableRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.ADMIN_CREATE_TABLE.getType(), request.toByteString());
            RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return AdminProto.CreateTableResponse.parseFrom(rpcResponse.getData());
        } catch (InvalidProtocolBufferException e) {
            LOG.error("InvalidProtocolBufferException", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public AdminProto.DropTableResponse dropTable(AdminProto.DropTableRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.ADMIN_DROP_TABLE.getType(), request.toByteString());
            RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return AdminProto.DropTableResponse.parseFrom(rpcResponse.getData());
        } catch (InvalidProtocolBufferException e) {
            LOG.error("InvalidProtocolBufferException", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public AdminProto.TruncateTableResponse truncateTable(AdminProto.TruncateTableRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.ADMIN_TRUNCATE_TABLE.getType(), request.toByteString());
            RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return AdminProto.TruncateTableResponse.parseFrom(rpcResponse.getData());
        } catch (InvalidProtocolBufferException e) {
            LOG.error("InvalidProtocolBufferException", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public AdminProto.ListTablesResponse listTables(AdminProto.ListTablesRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.ADMIN_LIST_TABLES.getType(), request.toByteString());
            RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return AdminProto.ListTablesResponse.parseFrom(rpcResponse.getData());
        } catch (InvalidProtocolBufferException e) {
            LOG.error("InvalidProtocolBufferException", e);
            throw new RuntimeException(e);
        }
    }

}
