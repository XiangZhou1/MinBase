package org.minbase.common.rpc.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.minbase.common.rpc.proto.generated.RpcProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RpcRequestDecoder extends ByteToMessageDecoder {
    private static final Logger logger = LoggerFactory.getLogger(RpcRequestDecoder.class);
    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) throws Exception {
        long len = byteBuf.getLong(0);
        byte[] bytes = new byte[(int) len];
        byteBuf.readBytes(bytes);
        final RpcProto.RpcRequest rpcRequest = RpcProto.RpcRequest.parseFrom(bytes);
        list.add(rpcRequest);
        logger.info("Decode rpcRequest:" + rpcRequest);
    }
}
