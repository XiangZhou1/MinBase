package org.minbase.client.service;

import com.google.protobuf.ByteString;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Promise;
import org.minbase.common.Constants;
import org.minbase.common.rpc.proto.generated.RpcProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class Service {
    private static final Logger LOG = LoggerFactory.getLogger(AdminService.class);
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

    protected RpcProto.RpcRequest buildRpcRequest(int type, ByteString data) {
        long id = requestId.incrementAndGet();
        RpcProto.RpcRequest.Builder builder = RpcProto.RpcRequest.newBuilder();
        return builder.setId(id).setCallType(type).setData(data).build();
    }

    protected RpcProto.RpcResponse call(RpcProto.RpcRequest rpcRequest) {
        Promise<RpcProto.RpcResponse> responsePromise = new DefaultPromise<>(group.next());
        waitingResponses.put(rpcRequest.getId(), responsePromise);
        channel.writeAndFlush(rpcRequest);
        try {
            final RpcProto.RpcResponse rpcResponse = responsePromise.get();
            if (rpcResponse.getCode() == -1) {
                throw new RuntimeException("rpc fail");
            }
            return rpcResponse;
        } catch (Exception e) {
            LOG.error("Call fail, callId:" + rpcRequest.getId() + ", callType:" + rpcRequest.getCallType(), e);
            throw new RuntimeException(e);
        }
    }

}
