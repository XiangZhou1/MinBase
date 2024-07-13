package org.minbase.common.rpc.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import io.netty.handler.codec.MessageToByteEncoder;
import org.minbase.common.rpc.RpcResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RpcResponseEncoder extends MessageToByteEncoder<RpcResponse> {
    private static final Logger logger = LoggerFactory.getLogger(RpcResponseEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, RpcResponse rpcResponse, ByteBuf byteBuf) throws Exception {
        byte[] bytes = rpcResponse.serialize();
        byteBuf.writeInt(bytes.length);
        byteBuf.writeBytes(bytes);
        logger.info("Encode rpcResponse:" + rpcResponse);
    }
}
