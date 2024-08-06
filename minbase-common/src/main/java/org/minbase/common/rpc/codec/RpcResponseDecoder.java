package org.minbase.common.rpc.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.minbase.common.rpc.proto.generated.RpcProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RpcResponseDecoder extends ByteToMessageDecoder {
    private static final Logger logger = LoggerFactory.getLogger(RpcResponseDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) throws Exception {
        long len = byteBuf.getLong(0);
        byte[] bytes = new byte[(int) len];
        byteBuf.readBytes(bytes);
        final RpcProto.RpcResponse rpcResponse = RpcProto.RpcResponse.parseFrom(bytes);
        list.add(rpcResponse);
        logger.info("Decode rpcResponse:" + rpcResponse);
    }
}
