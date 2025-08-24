package org.minbase.server.rpc;

import com.google.protobuf.ByteString;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.minbase.common.rpc.ResponseCode;
import org.minbase.common.rpc.proto.generated.AdminProto;
import org.minbase.common.rpc.proto.generated.ClientProto;
import org.minbase.common.rpc.proto.generated.RpcProto;
import org.minbase.common.rpc.service.CallType;
import org.minbase.server.table.TableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcHandler extends SimpleChannelInboundHandler<RpcProto.RpcRequest> {
    private static final Logger LOG = LoggerFactory.getLogger(RpcHandler.class);

    private RpcService service;

    public RpcHandler(TableManager server) {
        this.service = new RpcService(server);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, RpcProto.RpcRequest rpcRequest) throws Exception {
        LOG.info("RpcCall, callId:{}, callType:{}", rpcRequest.getId(), rpcRequest.getCallType());
        RpcProto.RpcResponse rpcResponse;
        try {
            int callType = rpcRequest.getCallType();
            if (callType == CallType.CLIENT_GET.getType()) {
                ClientProto.GetRequest getRequest = ClientProto.GetRequest.parseFrom(rpcRequest.getData());
                ClientProto.GetResponse getResponse = service.get(getRequest);
                rpcResponse = buildRpcResponse(ResponseCode.SUCCESS.getCode(), rpcRequest.getId(), getResponse.toByteString());
            } else if (callType == CallType.CLIENT_PUT.getType()) {
                ClientProto.PutRequest putRequest = ClientProto.PutRequest.parseFrom(rpcRequest.getData());
                ClientProto.PutResponse putResponse = service.put(putRequest);
                rpcResponse = buildRpcResponse(ResponseCode.SUCCESS.getCode(), rpcRequest.getId(), putResponse.toByteString());
            } else if (callType == CallType.CLIENT_CHECK_AND_PUT.getType()) {
                ClientProto.CheckAndPutRequest checkAndPutRequest = ClientProto.CheckAndPutRequest.parseFrom(rpcRequest.getData());
                ClientProto.CheckAndPutResponse putResponse = service.checkAndPut(checkAndPutRequest);
                rpcResponse = buildRpcResponse(ResponseCode.SUCCESS.getCode(), rpcRequest.getId(), putResponse.toByteString());
            } else if (callType == CallType.CLIENT_DELETE.getType()) {
                ClientProto.DeleteRequest deleteRequest = ClientProto.DeleteRequest.parseFrom(rpcRequest.getData());
                ClientProto.DeleteResponse deleteResponse = service.delete(deleteRequest);
                rpcResponse = buildRpcResponse(ResponseCode.SUCCESS.getCode(), rpcRequest.getId(), deleteResponse.toByteString());
            } else if (callType == CallType.CLIENT_BEGIN_TRANSACTION.getType()) {
                ClientProto.BeginTransactionRequest beginTransactionRequest = ClientProto.BeginTransactionRequest.parseFrom(rpcRequest.getData());
                ClientProto.BeginTransactionResponse beginTransactionResponse = service.beginTransaction(beginTransactionRequest);
                rpcResponse = buildRpcResponse(ResponseCode.SUCCESS.getCode(), rpcRequest.getId(), beginTransactionResponse.toByteString());
            } else if (callType == CallType.CLIENT_COMMIT.getType()) {
                ClientProto.CommitRequest commitRequest = ClientProto.CommitRequest.parseFrom(rpcRequest.getData());
                ClientProto.CommitResponse commitResponse = service.commit(commitRequest);
                rpcResponse = buildRpcResponse(ResponseCode.SUCCESS.getCode(), rpcRequest.getId(), commitResponse.toByteString());
            } else if (callType == CallType.CLIENT_ROLLBACK.getType()) {
                ClientProto.RollBackRequest rollBackRequest = ClientProto.RollBackRequest.parseFrom(rpcRequest.getData());
                ClientProto.RollBackResponse rollBackResponse = service.rollBack(rollBackRequest);
                rpcResponse = buildRpcResponse(ResponseCode.SUCCESS.getCode(), rpcRequest.getId(), rollBackResponse.toByteString());
            } else if (callType == CallType.TX_GET.getType()) {
                ClientProto.TxGetRequest txGetRequest = ClientProto.TxGetRequest.parseFrom(rpcRequest.getData());
                ClientProto.TxGetResponse txGetResponse = service.get(txGetRequest);
                rpcResponse = buildRpcResponse(ResponseCode.SUCCESS.getCode(), rpcRequest.getId(), txGetResponse.toByteString());
            } else if (callType == CallType.TX_PUT.getType()) {
                ClientProto.TxPutRequest txPutRequest = ClientProto.TxPutRequest.parseFrom(rpcRequest.getData());
                ClientProto.TxPutResponse txPutResponse = service.put(txPutRequest);
                rpcResponse = buildRpcResponse(ResponseCode.SUCCESS.getCode(), rpcRequest.getId(), txPutResponse.toByteString());
            } else if (callType == CallType.TX_CHECK_AND_PUT.getType()) {
                ClientProto.TxCheckAndPutRequest txCheckAndPutRequest = ClientProto.TxCheckAndPutRequest.parseFrom(rpcRequest.getData());
                ClientProto.TxCheckAndPutResponse txCheckAndPutResponse = service.checkAndPut(txCheckAndPutRequest);
                rpcResponse = buildRpcResponse(ResponseCode.SUCCESS.getCode(), rpcRequest.getId(), txCheckAndPutResponse.toByteString());
            } else if (callType == CallType.TX_DELETE.getType()) {
                ClientProto.TxDeleteRequest txDeleteRequest = ClientProto.TxDeleteRequest.parseFrom(rpcRequest.getData());
                ClientProto.TxDeleteResponse txDeleteResponse = service.delete(txDeleteRequest);
                rpcResponse = buildRpcResponse(ResponseCode.SUCCESS.getCode(), rpcRequest.getId(), txDeleteResponse.toByteString());
            } else if (callType == CallType.ADMIN_CREATE_TABLE.getType()) {
                AdminProto.CreateTableRequest createTableRequest = AdminProto.CreateTableRequest.parseFrom(rpcRequest.getData());
                AdminProto.CreateTableResponse createTableResponse = service.createTable(createTableRequest);
                rpcResponse = buildRpcResponse(ResponseCode.SUCCESS.getCode(), rpcRequest.getId(), createTableResponse.toByteString());
            } else if (callType == CallType.ADMIN_DROP_TABLE.getType()) {
                AdminProto.DropTableRequest dropTableRequest = AdminProto.DropTableRequest.parseFrom(rpcRequest.getData());
                AdminProto.DropTableResponse dropTableResponse = service.dropTable(dropTableRequest);
                rpcResponse = buildRpcResponse(ResponseCode.SUCCESS.getCode(), rpcRequest.getId(), dropTableResponse.toByteString());
            } else if (callType == CallType.ADMIN_TRUNCATE_TABLE.getType()) {
                AdminProto.TruncateTableRequest truncateTableRequest = AdminProto.TruncateTableRequest.parseFrom(rpcRequest.getData());
                AdminProto.TruncateTableResponse truncateTableResponse = service.truncateTable(truncateTableRequest);
                rpcResponse = buildRpcResponse(ResponseCode.SUCCESS.getCode(), rpcRequest.getId(), truncateTableResponse.toByteString());
            }  else if (callType == CallType.ADMIN_LIST_TABLES.getType()) {
                AdminProto.ListTablesRequest listTablesRequest = AdminProto.ListTablesRequest.parseFrom(rpcRequest.getData());
                AdminProto.ListTablesResponse listTablesResponse = service.listTables(listTablesRequest);
                rpcResponse = buildRpcResponse(ResponseCode.SUCCESS.getCode(), rpcRequest.getId(), listTablesResponse.toByteString());
            } else {
                rpcResponse = buildRpcResponse(ResponseCode.FAIL.getCode(), rpcRequest.getId(), ByteString.EMPTY);
            }
        } catch (Exception e) {
            rpcResponse = buildRpcResponse(ResponseCode.FAIL.getCode(), rpcRequest.getId(), ByteString.EMPTY);
            LOG.error("RpcCall error, callId:" + rpcRequest.getId() + ", callType:" + rpcRequest.getCallType(), e);
        }
        channelHandlerContext.writeAndFlush(rpcResponse);
    }

    private RpcProto.RpcResponse buildRpcResponse(int code, long requestId, ByteString data) {
        RpcProto.RpcResponse.Builder builder = RpcProto.RpcResponse.newBuilder();
        return builder.setCode(code).setId(requestId).setData(data).build();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        LOG.info("Connection ative, channel:{}", ctx.channel().remoteAddress());
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        service.rollBackTransactions();
        LOG.info("Connection inActive, channel:{}", ctx.channel().remoteAddress());
        super.channelInactive(ctx);
    }
}
