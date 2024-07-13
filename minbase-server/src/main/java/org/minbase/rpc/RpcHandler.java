package org.minbase.rpc;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.minbase.common.rpc.ResponseCode;
import org.minbase.common.rpc.RpcRequest;
import org.minbase.common.rpc.RpcResponse;
import org.minbase.common.rpc.service.ClientService;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.MinBaseServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.minbase.common.rpc.service.Methods.GET;
import static org.minbase.common.rpc.service.Methods.PUT;

public class RpcHandler extends SimpleChannelInboundHandler<RpcRequest> implements ClientService {
    private static final Logger logger = LoggerFactory.getLogger(RpcHandler.class);

    private MinBaseServer server;

    public RpcHandler(MinBaseServer minBaseServer) {
        this.server = minBaseServer;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, RpcRequest rpcRequest) throws Exception {
        logger.info("RpcHandler:" + rpcRequest);
        RpcResponse rpcResponse;
        try {
            if (GET.getName().equals(rpcRequest.getMethodName())) {
                final String bytes = get((String) rpcRequest.getArgs()[0]);
                rpcResponse = new RpcResponse(rpcRequest.getId(), ResponseCode.SUCCESS.getCode(), bytes);
            } else if (PUT.getName().equals(rpcRequest.getMethodName())) {
                String key = (String) rpcRequest.getArgs()[0];
                String value = (String) rpcRequest.getArgs()[1];
                put(key, value);
                rpcResponse = new RpcResponse(rpcRequest.getId(), ResponseCode.SUCCESS.getCode());
            } else {
                rpcResponse = new RpcResponse(rpcRequest.getId(), ResponseCode.FAIL.getCode());
            }
        } catch (Exception e) {
            rpcResponse = new RpcResponse(rpcRequest.getId(), ResponseCode.FAIL.getCode());
            logger.error("Rpc fail, rpcRequest=" + rpcRequest, e);
        }
        channelHandlerContext.writeAndFlush(rpcResponse);
    }

    @Override
    public String get(String key) {
        byte[] bytes = server.get(ByteUtil.toBytes(key));
        return new String(bytes);
    }

    @Override
    public void put(String key, String value) {
        server.put(ByteUtil.toBytes(key), ByteUtil.toBytes(value));
    }

    @Override
    public boolean checkAndPut(String checkKey, String checkValue, String key, String value) {
        return false;
    }

    @Override
    public void delete(String key) {

    }
}
