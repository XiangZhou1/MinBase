package org.minbase.server;


import org.minbase.rpc.RpcServer;
import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.lsmStorage.LsmStorage;
import org.minbase.server.op.WriteBatch;
import org.minbase.server.transaction.*;

import java.io.IOException;

public class MinBaseServer {
    private LsmStorage lsmStorage;
    private RpcServer rpcServer;

    public MinBaseServer() throws IOException {
        this.lsmStorage = new LsmStorage();
        this.rpcServer = new RpcServer(this);
    }

    public void startRpcServer() throws InterruptedException {
        this.rpcServer.start();
    }

    // 当前读
    public byte[] get(byte[] key) {
        return lsmStorage.get(key);
    }

    public KeyValueIterator scan(byte[] startKey, byte[] endKey) {
        return lsmStorage.scan(startKey, endKey);
    }


    public void put(byte[] key, byte[] value) {
        lsmStorage.put(key, value);
    }

    public void put(WriteBatch writeBatch) {
        lsmStorage.put(writeBatch);
    }

    public boolean checkAndPut(byte[] checkKey, byte[] checkValue, byte[] key, byte[] value) {
        return lsmStorage.checkAndPut(checkKey, checkValue, key, value);
    }

    public void delete(byte[] key) {
        lsmStorage.delete(key);
    }

    public Transaction newTransaction() {
        return TransactionManager.newTransaction(this.lsmStorage);
    }
}
