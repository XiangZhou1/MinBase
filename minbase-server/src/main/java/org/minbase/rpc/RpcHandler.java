package org.minbase.rpc;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.minbase.common.Constants;
import org.minbase.common.rpc.ResponseCode;
import org.minbase.common.rpc.proto.generated.AdminProto;
import org.minbase.common.rpc.proto.generated.ClientProto;
import org.minbase.common.rpc.proto.generated.RpcProto;
import org.minbase.common.rpc.service.CallType;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.MinBaseServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class RpcHandler extends SimpleChannelInboundHandler<RpcProto.RpcRequest> {
    private static final Logger logger = LoggerFactory.getLogger(RpcHandler.class);

    private RpcService service;

    public RpcHandler(MinBaseServer server) {
        this.service = new RpcService(server);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, RpcProto.RpcRequest rpcRequest) throws Exception {
        logger.info("RpcHandler:" + rpcRequest);
        RpcProto.RpcResponse rpcResponse;
        try {
            int callType = rpcRequest.getCallType();

            if (callType == CallType.CLIENT_GET.getType()) {
                ClientProto.GetRequest getRequest = ClientProto.GetRequest.parseFrom(rpcRequest.getData().getBytes(StandardCharsets.UTF_8));
                ClientProto.GetResponse getResponse = service.get(getRequest);
                rpcResponse = buildRpcResponse(ResponseCode.SUCCESS.getCode(), rpcRequest.getId(), getResponse.toByteString().toStringUtf8());
            } else if (callType == CallType.CLIENT_PUT.getType()) {
                ClientProto.PutRequest putRequest = ClientProto.PutRequest.parseFrom(ByteUtil.toBytes(rpcRequest.getData()));
                ClientProto.PutResponse putResponse = service.put(putRequest);
                rpcResponse = buildRpcResponse(ResponseCode.SUCCESS.getCode(), rpcRequest.getId(), putResponse.toByteString().toStringUtf8());
            } else if (callType == CallType.CLIENT_BEGIN_TRANSACTION.getType()) {
                ClientProto.BeginTransactionRequest beginTransactionRequest = ClientProto.BeginTransactionRequest.parseFrom(ByteUtil.toBytes(rpcRequest.getData()));
                ClientProto.BeginTransactionResponse beginTransactionResponse = service.beginTransaction(beginTransactionRequest);
                rpcResponse = buildRpcResponse(ResponseCode.SUCCESS.getCode(), rpcRequest.getId(), beginTransactionResponse.toByteString().toStringUtf8());
            } else if (callType == CallType.ADMIN_CREATE_TABLE.getType()) {
                AdminProto.CreateTableRequest createTableRequest = AdminProto.CreateTableRequest.parseFrom(ByteUtil.toBytes(rpcRequest.getData()));
                AdminProto.CreateTableResponse createTableResponse = service.createTable(createTableRequest);
                rpcResponse = buildRpcResponse(ResponseCode.SUCCESS.getCode(), rpcRequest.getId(), createTableResponse.toByteString().toStringUtf8());
            } else if (callType == CallType.TX_PUT.getType()) {
                ClientProto.TxPutRequest txPutRequest = ClientProto.TxPutRequest.parseFrom(ByteUtil.toBytes(rpcRequest.getData()));
                ClientProto.TxPutResponse txPutResponse = service.put(txPutRequest);
                rpcResponse = buildRpcResponse(ResponseCode.SUCCESS.getCode(), rpcRequest.getId(), txPutResponse.toByteString().toStringUtf8());
            } else {
                rpcResponse = buildRpcResponse(ResponseCode.FAIL.getCode(), rpcRequest.getId(), "");
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

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        service.rollBackTransactions();
        super.channelInactive(ctx);
    }


}
