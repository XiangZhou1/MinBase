package org.minbase.client.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.Promise;
import org.minbase.common.rpc.RpcResponse;

import java.util.concurrent.ConcurrentHashMap;

public class ClientHandler extends SimpleChannelInboundHandler<RpcResponse> {
    private ConcurrentHashMap<Long, Promise<RpcResponse>> waitingResponses;
    public ClientHandler(ConcurrentHashMap<Long, Promise<RpcResponse>> waitingResponses) {
        this.waitingResponses = waitingResponses;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, RpcResponse rpcResponse) throws Exception {
        final Promise<RpcResponse> responsePromise = waitingResponses.get(rpcResponse.getId());
        if (responsePromise != null) {
            responsePromise.setSuccess(rpcResponse);
        }
    }
}
