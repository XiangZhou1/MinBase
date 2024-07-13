package org.minbase.common.rpc.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import io.netty.handler.codec.MessageToByteEncoder;
import org.minbase.common.rpc.RpcRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RpcRequestEncoder extends MessageToByteEncoder<RpcRequest> {
    private static final Logger logger = LoggerFactory.getLogger(RpcRequestEncoder.class);
    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, RpcRequest rpcRequest, ByteBuf byteBuf) throws Exception {
        byte[] bytes = rpcRequest.serialize();
        byteBuf.writeInt(bytes.length);
        byteBuf.writeBytes(bytes);
        logger.info("Encode rpcRequest:" + rpcRequest);
    }
}
