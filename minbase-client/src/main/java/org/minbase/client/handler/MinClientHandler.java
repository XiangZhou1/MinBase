package org.minbase.client.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.Promise;
import org.minbase.common.rpc.proto.generated.RpcProto;

import java.util.concurrent.ConcurrentHashMap;

public class MinClientHandler extends SimpleChannelInboundHandler<RpcProto.RpcResponse> {
    private final ConcurrentHashMap<Long, Promise<RpcProto.RpcResponse>> waitingResponses;

    public MinClientHandler(ConcurrentHashMap<Long, Promise<RpcProto.RpcResponse>> waitingResponses) {
        this.waitingResponses = waitingResponses;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, RpcProto.RpcResponse rpcResponse) {
        final Promise<RpcProto.RpcResponse> responsePromise = waitingResponses.get(rpcResponse.getId());
        if (responsePromise != null) {
            responsePromise.setSuccess(rpcResponse);
        }
    }
}
