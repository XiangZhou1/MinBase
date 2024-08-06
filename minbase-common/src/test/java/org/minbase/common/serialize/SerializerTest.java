package org.minbase.common.serialize;

import org.junit.Test;

public class SerializerTest {
    @Test
    public void test1() {
        RpcRequest rpcRequest = new RpcRequest(1, "get", new Object[]{"get"});
        final byte[] bytes = rpcRequest.serialize();

        RpcRequest rpcRequest1 = RpcRequest.deSerialize(bytes);
        System.out.println(rpcRequest1);
        final String arg = (String) rpcRequest1.getArgs()[0];
    }

}
