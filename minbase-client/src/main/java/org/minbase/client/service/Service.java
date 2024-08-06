package org.minbase.client.service;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Promise;
import org.minbase.common.rpc.proto.generated.RpcProto;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class Service {
    protected Channel channel;
    protected AtomicLong requestId;
    protected ConcurrentHashMap<Long, Promise<RpcProto.RpcResponse>> waitingResponses;
    protected EventLoopGroup group;

    public Service(Channel channel, AtomicLong requestId, ConcurrentHashMap<Long, Promise<RpcProto.RpcResponse>> waitingResponses, EventLoopGroup group) {
        this.channel = channel;
        this.requestId = requestId;
        this.waitingResponses = waitingResponses;
        this.group = group;
    }

    protected RpcProto.RpcResponse call(RpcProto.RpcRequest rpcRequest) {
        Promise<RpcProto.RpcResponse> responsePromise = new DefaultPromise<>(group.next());
        waitingResponses.put(rpcRequest.getId(), responsePromise);
        channel.writeAndFlush(rpcRequest.toByteString().toStringUtf8());
        try {
            final RpcProto.RpcResponse rpcResponse = responsePromise.get();
            if (rpcResponse.getCode() == -1) {
                throw new RuntimeException("rpc fail");
            }
            return rpcResponse;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
