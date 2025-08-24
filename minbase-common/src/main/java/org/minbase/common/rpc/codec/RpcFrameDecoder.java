package org.minbase.common.rpc.codec;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class RpcFrameDecoder extends LengthFieldBasedFrameDecoder {
    public RpcFrameDecoder(int maxFrameLength, int lengthFieldOffset, int lengthFieldLength) {
        super(maxFrameLength, lengthFieldOffset, lengthFieldLength);
    }

    public RpcFrameDecoder() {
        super(Integer.MAX_VALUE, 0, 4, 0, 4);
    }
}
