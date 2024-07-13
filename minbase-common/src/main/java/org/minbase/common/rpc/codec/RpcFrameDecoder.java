package org.minbase.common.rpc.codec;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class RpcFrameDecoder extends LengthFieldBasedFrameDecoder {
    public RpcFrameDecoder(int maxFrameLength, int lengthFieldOffset, int lengthFieldLength) {
        super(maxFrameLength, lengthFieldOffset, lengthFieldLength);
    }

    public RpcFrameDecoder() {
        super(10000000, 0, 4);
    }
}
