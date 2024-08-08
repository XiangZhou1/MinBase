package org.minbase.client.service;

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Promise;
import org.minbase.common.rpc.proto.generated.ClientProto;
import org.minbase.common.rpc.proto.generated.RpcProto;
import org.minbase.common.rpc.proto.generated.TransactionServiceGrpc;
import org.minbase.common.rpc.service.CallType;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class TxService extends Service implements TransactionServiceGrpc.TransactionServiceBlockingClient {
    public TxService(Channel channel, AtomicLong requestId, ConcurrentHashMap<Long, Promise<RpcProto.RpcResponse>> waitingResponses, EventLoopGroup group) {
        super(channel, requestId, waitingResponses, group);
    }

    @Override
    public ClientProto.TxGetResponse get(ClientProto.TxGetRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.TX_GET.getType(), request.toByteString().toStringUtf8());
            RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return ClientProto.TxGetResponse.parseFrom(rpcResponse.getValueBytes().toByteArray());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public ClientProto.TxPutResponse put(ClientProto.TxPutRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.TX_PUT.getType(), request.toByteString().toStringUtf8());
            RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return ClientProto.TxPutResponse.parseFrom(rpcResponse.getValueBytes().toByteArray());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public ClientProto.TxCheckAndPutResponse checkAndPut(ClientProto.TxCheckAndPutRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.TX_PUT.getType(), request.toByteString().toStringUtf8());
            RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return ClientProto.TxCheckAndPutResponse.parseFrom(rpcResponse.getValueBytes().toByteArray());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public ClientProto.TxDeleteResponse delete(ClientProto.TxDeleteRequest request) {
        try {
            RpcProto.RpcRequest rpcRequest = buildRpcRequest(CallType.TX_PUT.getType(), request.toByteString().toStringUtf8());
            RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return ClientProto.TxDeleteResponse.parseFrom(rpcResponse.getValueBytes().toByteArray());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return null;
    }
}
