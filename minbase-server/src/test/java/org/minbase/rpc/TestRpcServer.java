package org.minbase.rpc;

import org.junit.Test;
import org.minbase.common.conf.Configuration;
import org.minbase.server.MinBaseServer;
import org.minbase.server.rpc.RpcServer;
import org.minbase.server.table.TableManager;

public class TestRpcServer {
    @Test
    public void test1() throws Exception{
        MinBaseServer.startServer(null);
    }
}
