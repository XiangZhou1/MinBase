package org.minbase.common.rpc.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.minbase.common.rpc.proto.generated.RpcProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcResponseEncoder extends MessageToByteEncoder<RpcProto.RpcResponse> {
    private static final Logger LOG = LoggerFactory.getLogger(RpcRequestEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, RpcProto.RpcResponse rpcResponse, ByteBuf byteBuf) throws Exception {
        byte[] bytes = rpcResponse.toByteArray();
        byteBuf.writeInt(bytes.length);
        byteBuf.writeBytes(bytes);
    }
}
