package org.minbase.client.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.Promise;
import org.minbase.client.service.AdminService;
import org.minbase.common.rpc.proto.generated.RpcProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class MinClientHandler extends SimpleChannelInboundHandler<RpcProto.RpcResponse> {
    private static final Logger LOG = LoggerFactory.getLogger(AdminService.class);
    private ConcurrentHashMap<Long, Promise<RpcProto.RpcResponse>> waitingResponses;

    public MinClientHandler(ConcurrentHashMap<Long, Promise<RpcProto.RpcResponse>> waitingResponses) {
        this.waitingResponses = waitingResponses;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, RpcProto.RpcResponse rpcResponse) throws Exception {
        LOG.debug("Receive rpcResponse, callId:{}", rpcResponse.getId());
        final Promise<RpcProto.RpcResponse> responsePromise = waitingResponses.get(rpcResponse.getId());
        if (responsePromise != null) {
            responsePromise.setSuccess(rpcResponse);
        }
    }
}
