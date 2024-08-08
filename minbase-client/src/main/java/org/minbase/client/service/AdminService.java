package org.minbase.client.service;

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Promise;
import org.minbase.common.rpc.proto.generated.AdminProto;
import org.minbase.common.rpc.proto.generated.AdminServiceGrpc;
import org.minbase.common.rpc.proto.generated.ClientProto;
import org.minbase.common.rpc.proto.generated.RpcProto;
import org.minbase.common.rpc.service.CallType;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class AdminService extends Service implements AdminServiceGrpc.AdminServiceBlockingClient {
    public AdminService(Channel channel, AtomicLong requestId, ConcurrentHashMap<Long, Promise<RpcProto.RpcResponse>> waitingResponses, EventLoopGroup group) {
        super(channel, requestId, waitingResponses, group);
    }

    @Override
    public AdminProto.CreateTableResponse createTable(AdminProto.CreateTableRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.ADMIN_CREATE_TABLE.getType(), request.toByteString().toStringUtf8());
            RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return AdminProto.CreateTableResponse.parseFrom(rpcResponse.getValueBytes().toByteArray());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public AdminProto.DropTableResponse dropTable(AdminProto.DropTableRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.CLIENT_GET.getType(), request.toByteString().toStringUtf8());
            RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return AdminProto.DropTableResponse.parseFrom(rpcResponse.getValueBytes().toByteArray());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public AdminProto.TruncateTableResponse truncateTable(AdminProto.TruncateTableRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.CLIENT_GET.getType(), request.toByteString().toStringUtf8());
            RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return AdminProto.TruncateTableResponse.parseFrom(rpcResponse.getValueBytes().toByteArray());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return null;
    }
}
