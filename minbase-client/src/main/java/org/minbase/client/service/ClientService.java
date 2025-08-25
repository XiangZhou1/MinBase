package org.minbase.client.service;

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Promise;
import org.minbase.common.Constants;
import org.minbase.common.rpc.proto.generated.ClientProto;
import org.minbase.common.rpc.proto.generated.ClientServiceGrpc;
import org.minbase.common.rpc.proto.generated.RpcProto;
import org.minbase.common.rpc.service.CallType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ClientService extends Service implements ClientServiceGrpc.ClientServiceBlockingClient {
    private static final Logger LOG = LoggerFactory.getLogger(AdminService.class);
    public ClientService(Channel channel, AtomicLong requestId, ConcurrentHashMap<Long, Promise<RpcProto.RpcResponse>> waitingResponses, EventLoopGroup group) {
        super(channel, requestId, waitingResponses, group);
    }

    @Override
    public ClientProto.GetResponse get(ClientProto.GetRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.CLIENT_GET.getType(), request.toByteString());
            RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return ClientProto.GetResponse.parseFrom(rpcResponse.getData());
        } catch (InvalidProtocolBufferException e) {
            LOG.error("InvalidProtocolBufferException", e);
            throw new RuntimeException(e);
        }
    }


    @Override
    public ClientProto.PutResponse put(ClientProto.PutRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.CLIENT_PUT.getType(), request.toByteString());
            final RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return ClientProto.PutResponse.parseFrom(rpcResponse.getData());
        } catch (InvalidProtocolBufferException e) {
            LOG.error("InvalidProtocolBufferException", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public ClientProto.CheckAndPutResponse checkAndPut(ClientProto.CheckAndPutRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.CLIENT_CHECK_AND_PUT.getType(), request.toByteString());
            final RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return ClientProto.CheckAndPutResponse.parseFrom(rpcResponse.getData());
        } catch (InvalidProtocolBufferException e) {
            LOG.error("InvalidProtocolBufferException", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public ClientProto.DeleteResponse delete(ClientProto.DeleteRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.CLIENT_DELETE.getType(), request.toByteString());
            final RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return ClientProto.DeleteResponse.parseFrom(rpcResponse.getData());
        } catch (InvalidProtocolBufferException e) {
            LOG.error("InvalidProtocolBufferException", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public ClientProto.BeginTransactionResponse beginTransaction(ClientProto.BeginTransactionRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.CLIENT_BEGIN_TRANSACTION.getType(), request.toByteString());
            final RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return ClientProto.BeginTransactionResponse.parseFrom(rpcResponse.getData());
        } catch (InvalidProtocolBufferException e) {
            LOG.error("InvalidProtocolBufferException", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public ClientProto.RollBackResponse rollBack(ClientProto.RollBackRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.CLIENT_ROLLBACK.getType(), request.toByteString());
            final RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return ClientProto.RollBackResponse.parseFrom(rpcResponse.getData());
        } catch (InvalidProtocolBufferException e) {
            LOG.error("InvalidProtocolBufferException", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public ClientProto.CommitResponse commit(ClientProto.CommitRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.CLIENT_COMMIT.getType(), request.toByteString());
            final RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return ClientProto.CommitResponse.parseFrom(rpcResponse.getData());
        } catch (InvalidProtocolBufferException e) {
            LOG.error("InvalidProtocolBufferException", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public ClientProto.ScanResponse scan(ClientProto.ScanRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.CLIENT_SCAN.getType(), request.toByteString());
            final RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return ClientProto.ScanResponse.parseFrom(rpcResponse.getData());
        } catch (InvalidProtocolBufferException e) {
            LOG.error("InvalidProtocolBufferException", e);
            throw new RuntimeException(e);
        }
    }
}
