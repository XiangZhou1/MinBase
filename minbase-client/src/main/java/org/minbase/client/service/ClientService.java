package org.minbase.client.service;

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Promise;
import org.minbase.common.rpc.proto.generated.ClientProto;
import org.minbase.common.rpc.proto.generated.ClientServiceGrpc;
import org.minbase.common.rpc.proto.generated.RpcProto;
import org.minbase.common.rpc.service.CallType;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ClientService extends Service implements ClientServiceGrpc.ClientServiceBlockingClient {
    public ClientService(Channel channel, AtomicLong requestId, ConcurrentHashMap<Long, Promise<RpcProto.RpcResponse>> waitingResponses, EventLoopGroup group) {
        super(channel, requestId, waitingResponses, group);
    }

    @Override
    public ClientProto.GetResponse get(ClientProto.GetRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.CLIENT_GET.getType(), request.toByteString().toStringUtf8());
            RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return ClientProto.GetResponse.parseFrom(rpcResponse.getValueBytes().toByteArray());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public ClientProto.PutResponse put(ClientProto.PutRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.CLIENT_PUT.getType(), request.toByteString().toStringUtf8());
            final RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return ClientProto.PutResponse.parseFrom(rpcResponse.getValueBytes().toByteArray());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public ClientProto.CheckAndPutResponse checkAndPut(ClientProto.CheckAndPutRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.CLIENT_CHECK_AND_PUT.getType(), request.toByteString().toStringUtf8());
            final RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return ClientProto.CheckAndPutResponse.parseFrom(rpcResponse.getValueBytes().toByteArray());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public ClientProto.DeleteResponse delete(ClientProto.DeleteRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.CLIENT_DELETE.getType(), request.toByteString().toStringUtf8());
            final RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return ClientProto.DeleteResponse.parseFrom(rpcResponse.getValueBytes().toByteArray());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public ClientProto.BeginTransactionResponse beginTransaction(ClientProto.BeginTransactionRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.CLIENT_BEGIN_TRANSACTION.getType(), request.toByteString().toStringUtf8());
            final RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return ClientProto.BeginTransactionResponse.parseFrom(rpcResponse.getValueBytes().toByteArray());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public ClientProto.RollBackResponse rollBack(ClientProto.RollBackRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.CLIENT_ROLLBACK.getType(), request.toByteString().toStringUtf8());
            final RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return ClientProto.RollBackResponse.parseFrom(rpcResponse.getValueBytes().toByteArray());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public ClientProto.CommitResponse commit(ClientProto.CommitRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.CLIENT_COMMIT.getType(), request.toByteString().toStringUtf8());
            final RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return ClientProto.CommitResponse.parseFrom(rpcResponse.getValueBytes().toByteArray());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return null;
    }
}
