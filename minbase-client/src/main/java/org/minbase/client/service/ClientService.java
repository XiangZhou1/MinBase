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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ClientService extends Service implements ClientServiceGrpc.ClientServiceBlockingClient {
    public ClientService(Channel channel, AtomicLong requestId, ConcurrentHashMap<Long, Promise<RpcProto.RpcResponse>> waitingResponses, EventLoopGroup group) {
        super(channel, requestId, waitingResponses, group);
    }

    @Override
    public ClientProto.GetResponse get(ClientProto.GetRequest request) {
        try {
            long length = 0;
            final String data = request.toByteString().toStringUtf8();
            length += data.length();
            length += Constants.LONG_LENGTH;
            length += Constants.INTEGER_LENGTH;
            final long id = requestId.incrementAndGet();

            final RpcProto.RpcRequest.Builder builder = RpcProto.RpcRequest.newBuilder();
            final RpcProto.RpcRequest rpcRequest = builder.setLength(length).setId(id).setCallType(CallType.CLIENT_GET.getType()).setData(data).build();
            final RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return ClientProto.GetResponse.parseFrom(rpcResponse.getValueBytes().toByteArray());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public ClientProto.PutResponse put(ClientProto.PutRequest request) {
        try {
            long length = 0;
            final String data = request.toByteString().toStringUtf8();
            length += data.length();
            length += Constants.LONG_LENGTH;
            length += Constants.INTEGER_LENGTH;
            final long id = requestId.incrementAndGet();

            final RpcProto.RpcRequest.Builder builder = RpcProto.RpcRequest.newBuilder();
            final RpcProto.RpcRequest rpcRequest = builder.setLength(length).setId(id).setCallType(CallType.CLIENT_PUT.getType()).setData(request.toByteString().toStringUtf8()).build();
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
            long length = 0;
            final String data = request.toByteString().toStringUtf8();
            length += data.length();
            length += Constants.LONG_LENGTH;
            length += Constants.INTEGER_LENGTH;
            final long id = requestId.incrementAndGet();

            final RpcProto.RpcRequest.Builder builder = RpcProto.RpcRequest.newBuilder();
            final RpcProto.RpcRequest rpcRequest = builder.setLength(length).setId(id).setCallType(CallType.CLIENT_GET.getType()).setData(request.toByteString().toStringUtf8()).build();
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
            long length = 0;
            final String data = request.toByteString().toStringUtf8();
            length += data.length();
            length += Constants.LONG_LENGTH;
            length += Constants.INTEGER_LENGTH;
            final long id = requestId.incrementAndGet();

            final RpcProto.RpcRequest.Builder builder = RpcProto.RpcRequest.newBuilder();
            final RpcProto.RpcRequest rpcRequest = builder.setLength(length).setId(id).setCallType(CallType.CLIENT_GET.getType()).setData(request.toByteString().toStringUtf8()).build();
            final RpcProto.RpcResponse rpcResponse = call(rpcRequest);
            return ClientProto.DeleteResponse.parseFrom(rpcResponse.getValueBytes().toByteArray());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return null;
    }

}
