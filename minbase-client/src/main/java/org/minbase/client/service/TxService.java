package org.minbase.client.service;

import org.minbase.common.rpc.proto.generated.ClientProto;
import org.minbase.common.rpc.proto.generated.RpcProto;
import org.minbase.common.rpc.proto.generated.TransactionServiceGrpc;

public class TxService extends Service implements TransactionServiceGrpc.TransactionServiceBlockingClient {
    @Override
    public ClientProto.TxGetResponse get(ClientProto.TxGetRequest request) {
        RpcProto.RpcRequest.Builder builder = RpcProto.RpcRequest.newBuilder();
        builder.setId()
        return null;
    }

    @Override
    public ClientProto.TxPutResponse put(ClientProto.TxPutRequest request) {
        return null;
    }

    @Override
    public ClientProto.TxCheckAndPutResponse checkAndPut(ClientProto.TxCheckAndPutRequest request) {
        return null;
    }

    @Override
    public ClientProto.TxDeleteResponse delete(ClientProto.TxDeleteRequest request) {
        return null;
    }
}
