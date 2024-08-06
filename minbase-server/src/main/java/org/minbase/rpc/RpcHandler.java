package org.minbase.rpc;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.minbase.common.Constants;
import org.minbase.common.rpc.ResponseCode;
import org.minbase.common.rpc.proto.generated.ClientProto;
import org.minbase.common.rpc.proto.generated.ClientServiceGrpc;
import org.minbase.common.rpc.proto.generated.RpcProto;
import org.minbase.common.rpc.service.CallType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class RpcHandler extends SimpleChannelInboundHandler<RpcProto.RpcRequest> {
    private static final Logger logger = LoggerFactory.getLogger(RpcHandler.class);

    private ClientServiceGrpc.ClientServiceBlockingClient service;

    public RpcHandler(ClientServiceGrpc.ClientServiceBlockingClient service) {
        this.service = service;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, RpcProto.RpcRequest rpcRequest) throws Exception {
        logger.info("RpcHandler:" + rpcRequest);
        RpcProto.RpcResponse rpcResponse;
        try {
            final int callType = rpcRequest.getCallType();
            if (callType == CallType.CLIENT_GET.getType()) {
                ClientProto.GetRequest getRequest = ClientProto.GetRequest.parseFrom(rpcRequest.getData().getBytes(StandardCharsets.UTF_8));
                ClientProto.GetResponse getResponse = service.get(getRequest);
                rpcResponse = buildRpcResponse(ResponseCode.SUCCESS.getCode(), rpcRequest.getId(), getResponse.toByteString().toStringUtf8());
            } else if (callType == CallType.CLIENT_PUT.getType()) {
                ClientProto.PutRequest putRequest = ClientProto.PutRequest.parseFrom(rpcRequest.getData().getBytes(StandardCharsets.UTF_8));
                ClientProto.PutResponse putResponse = service.put(putRequest);
                rpcResponse = buildRpcResponse(ResponseCode.SUCCESS.getCode(), rpcRequest.getId(), putResponse.toByteString().toStringUtf8());
            } else {
                throw new RuntimeException("");
            }
        } catch (Exception e) {
            rpcResponse = buildRpcResponse(ResponseCode.FAIL.getCode(), rpcRequest.getId(), "");
            logger.error("Rpc fail, rpcRequest=" + rpcRequest, e);
        }
        channelHandlerContext.writeAndFlush(rpcResponse);
    }


    private RpcProto.RpcResponse buildRpcResponse(int code, long requestId, String data) {
        long length = 0;
        length += Constants.INTEGER_LENGTH;
        length += Constants.LONG_LENGTH;
        length += data.length();

        RpcProto.RpcResponse.Builder builder = RpcProto.RpcResponse.newBuilder();
        return builder.setCode(1).setId(requestId).setValue(data).setLength(length).build();
    }
}
