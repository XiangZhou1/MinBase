package org.minbase.rpc;

import org.junit.Test;
import org.minbase.server.MinBaseServer;
import org.minbase.server.conf.Configuration;
import org.minbase.server.rpc.RpcServer;

public class TestRpcServer {
    @Test
    public void test1() throws Exception{
        RpcServer rpcServer = new RpcServer(new MinBaseServer(new Configuration()));
        rpcServer.start();
    }
}
