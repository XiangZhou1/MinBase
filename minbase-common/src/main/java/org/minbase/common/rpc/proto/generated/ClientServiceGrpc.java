package org.minbase.common.rpc.proto.generated;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;

@javax.annotation.Generated("by gRPC proto compiler")
public class ClientServiceGrpc {

  private ClientServiceGrpc() {}

  public static final String SERVICE_NAME = "org.minbase.common.rpc.proto.generated.ClientService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<ClientProto.GetRequest,
      ClientProto.GetResponse> METHOD_GET =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.ClientService", "get"),
          io.grpc.protobuf.ProtoUtils.marshaller(ClientProto.GetRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(ClientProto.GetResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<ClientProto.PutRequest,
      ClientProto.PutResponse> METHOD_PUT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.ClientService", "put"),
          io.grpc.protobuf.ProtoUtils.marshaller(ClientProto.PutRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(ClientProto.PutResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<ClientProto.CheckAndPutRequest,
      ClientProto.CheckAndPutResponse> METHOD_CHECK_AND_PUT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.ClientService", "checkAndPut"),
          io.grpc.protobuf.ProtoUtils.marshaller(ClientProto.CheckAndPutRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(ClientProto.CheckAndPutResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<ClientProto.DeleteRequest,
      ClientProto.DeleteResponse> METHOD_DELETE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.ClientService", "delete"),
          io.grpc.protobuf.ProtoUtils.marshaller(ClientProto.DeleteRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(ClientProto.DeleteResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<ClientProto.BeginTransactionRequest,
      ClientProto.BeginTransactionResponse> METHOD_BEGIN_TRANSACTION =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.ClientService", "beginTransaction"),
          io.grpc.protobuf.ProtoUtils.marshaller(ClientProto.BeginTransactionRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(ClientProto.BeginTransactionResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<ClientProto.RollBackRequest,
      ClientProto.RollBackResponse> METHOD_ROLL_BACK =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.ClientService", "rollBack"),
          io.grpc.protobuf.ProtoUtils.marshaller(ClientProto.RollBackRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(ClientProto.RollBackResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<ClientProto.CommitRequest,
      ClientProto.CommitResponse> METHOD_COMMIT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.ClientService", "commit"),
          io.grpc.protobuf.ProtoUtils.marshaller(ClientProto.CommitRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(ClientProto.CommitResponse.getDefaultInstance()));

  public static ClientServiceStub newStub(io.grpc.Channel channel) {
    return new ClientServiceStub(channel);
  }

  public static ClientServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new ClientServiceBlockingStub(channel);
  }

  public static ClientServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new ClientServiceFutureStub(channel);
  }

  public static interface ClientService {

    public void get(ClientProto.GetRequest request,
                    io.grpc.stub.StreamObserver<ClientProto.GetResponse> responseObserver);

    public void put(ClientProto.PutRequest request,
                    io.grpc.stub.StreamObserver<ClientProto.PutResponse> responseObserver);

    public void checkAndPut(ClientProto.CheckAndPutRequest request,
                            io.grpc.stub.StreamObserver<ClientProto.CheckAndPutResponse> responseObserver);

    public void delete(ClientProto.DeleteRequest request,
                       io.grpc.stub.StreamObserver<ClientProto.DeleteResponse> responseObserver);

    public void beginTransaction(ClientProto.BeginTransactionRequest request,
                                 io.grpc.stub.StreamObserver<ClientProto.BeginTransactionResponse> responseObserver);

    public void rollBack(ClientProto.RollBackRequest request,
                         io.grpc.stub.StreamObserver<ClientProto.RollBackResponse> responseObserver);

    public void commit(ClientProto.CommitRequest request,
                       io.grpc.stub.StreamObserver<ClientProto.CommitResponse> responseObserver);
  }

  public static interface ClientServiceBlockingClient {

    public ClientProto.GetResponse get(ClientProto.GetRequest request);

    public ClientProto.PutResponse put(ClientProto.PutRequest request);

    public ClientProto.CheckAndPutResponse checkAndPut(ClientProto.CheckAndPutRequest request);

    public ClientProto.DeleteResponse delete(ClientProto.DeleteRequest request);

    public ClientProto.BeginTransactionResponse beginTransaction(ClientProto.BeginTransactionRequest request);

    public ClientProto.RollBackResponse rollBack(ClientProto.RollBackRequest request);

    public ClientProto.CommitResponse commit(ClientProto.CommitRequest request);
  }

  public static interface ClientServiceFutureClient {

    public com.google.common.util.concurrent.ListenableFuture<ClientProto.GetResponse> get(
        ClientProto.GetRequest request);

    public com.google.common.util.concurrent.ListenableFuture<ClientProto.PutResponse> put(
        ClientProto.PutRequest request);

    public com.google.common.util.concurrent.ListenableFuture<ClientProto.CheckAndPutResponse> checkAndPut(
        ClientProto.CheckAndPutRequest request);

    public com.google.common.util.concurrent.ListenableFuture<ClientProto.DeleteResponse> delete(
        ClientProto.DeleteRequest request);

    public com.google.common.util.concurrent.ListenableFuture<ClientProto.BeginTransactionResponse> beginTransaction(
        ClientProto.BeginTransactionRequest request);

    public com.google.common.util.concurrent.ListenableFuture<ClientProto.RollBackResponse> rollBack(
        ClientProto.RollBackRequest request);

    public com.google.common.util.concurrent.ListenableFuture<ClientProto.CommitResponse> commit(
        ClientProto.CommitRequest request);
  }

  public static class ClientServiceStub extends io.grpc.stub.AbstractStub<ClientServiceStub>
      implements ClientService {
    private ClientServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ClientServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected ClientServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ClientServiceStub(channel, callOptions);
    }

    @Override
    public void get(ClientProto.GetRequest request,
                    io.grpc.stub.StreamObserver<ClientProto.GetResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET, getCallOptions()), request, responseObserver);
    }

    @Override
    public void put(ClientProto.PutRequest request,
                    io.grpc.stub.StreamObserver<ClientProto.PutResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_PUT, getCallOptions()), request, responseObserver);
    }

    @Override
    public void checkAndPut(ClientProto.CheckAndPutRequest request,
                            io.grpc.stub.StreamObserver<ClientProto.CheckAndPutResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CHECK_AND_PUT, getCallOptions()), request, responseObserver);
    }

    @Override
    public void delete(ClientProto.DeleteRequest request,
                       io.grpc.stub.StreamObserver<ClientProto.DeleteResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DELETE, getCallOptions()), request, responseObserver);
    }

    @Override
    public void beginTransaction(ClientProto.BeginTransactionRequest request,
                                 io.grpc.stub.StreamObserver<ClientProto.BeginTransactionResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_BEGIN_TRANSACTION, getCallOptions()), request, responseObserver);
    }

    @Override
    public void rollBack(ClientProto.RollBackRequest request,
                         io.grpc.stub.StreamObserver<ClientProto.RollBackResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_ROLL_BACK, getCallOptions()), request, responseObserver);
    }

    @Override
    public void commit(ClientProto.CommitRequest request,
                       io.grpc.stub.StreamObserver<ClientProto.CommitResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_COMMIT, getCallOptions()), request, responseObserver);
    }
  }

  public static class ClientServiceBlockingStub extends io.grpc.stub.AbstractStub<ClientServiceBlockingStub>
      implements ClientServiceBlockingClient {
    private ClientServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ClientServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected ClientServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ClientServiceBlockingStub(channel, callOptions);
    }

    @Override
    public ClientProto.GetResponse get(ClientProto.GetRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET, getCallOptions(), request);
    }

    @Override
    public ClientProto.PutResponse put(ClientProto.PutRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_PUT, getCallOptions(), request);
    }

    @Override
    public ClientProto.CheckAndPutResponse checkAndPut(ClientProto.CheckAndPutRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CHECK_AND_PUT, getCallOptions(), request);
    }

    @Override
    public ClientProto.DeleteResponse delete(ClientProto.DeleteRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DELETE, getCallOptions(), request);
    }

    @Override
    public ClientProto.BeginTransactionResponse beginTransaction(ClientProto.BeginTransactionRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_BEGIN_TRANSACTION, getCallOptions(), request);
    }

    @Override
    public ClientProto.RollBackResponse rollBack(ClientProto.RollBackRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_ROLL_BACK, getCallOptions(), request);
    }

    @Override
    public ClientProto.CommitResponse commit(ClientProto.CommitRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_COMMIT, getCallOptions(), request);
    }
  }

  public static class ClientServiceFutureStub extends io.grpc.stub.AbstractStub<ClientServiceFutureStub>
      implements ClientServiceFutureClient {
    private ClientServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ClientServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected ClientServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ClientServiceFutureStub(channel, callOptions);
    }

    @Override
    public com.google.common.util.concurrent.ListenableFuture<ClientProto.GetResponse> get(
        ClientProto.GetRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET, getCallOptions()), request);
    }

    @Override
    public com.google.common.util.concurrent.ListenableFuture<ClientProto.PutResponse> put(
        ClientProto.PutRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_PUT, getCallOptions()), request);
    }

    @Override
    public com.google.common.util.concurrent.ListenableFuture<ClientProto.CheckAndPutResponse> checkAndPut(
        ClientProto.CheckAndPutRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CHECK_AND_PUT, getCallOptions()), request);
    }

    @Override
    public com.google.common.util.concurrent.ListenableFuture<ClientProto.DeleteResponse> delete(
        ClientProto.DeleteRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DELETE, getCallOptions()), request);
    }

    @Override
    public com.google.common.util.concurrent.ListenableFuture<ClientProto.BeginTransactionResponse> beginTransaction(
        ClientProto.BeginTransactionRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_BEGIN_TRANSACTION, getCallOptions()), request);
    }

    @Override
    public com.google.common.util.concurrent.ListenableFuture<ClientProto.RollBackResponse> rollBack(
        ClientProto.RollBackRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_ROLL_BACK, getCallOptions()), request);
    }

    @Override
    public com.google.common.util.concurrent.ListenableFuture<ClientProto.CommitResponse> commit(
        ClientProto.CommitRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_COMMIT, getCallOptions()), request);
    }
  }

  private static final int METHODID_GET = 0;
  private static final int METHODID_PUT = 1;
  private static final int METHODID_CHECK_AND_PUT = 2;
  private static final int METHODID_DELETE = 3;
  private static final int METHODID_BEGIN_TRANSACTION = 4;
  private static final int METHODID_ROLL_BACK = 5;
  private static final int METHODID_COMMIT = 6;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ClientService serviceImpl;
    private final int methodId;

    public MethodHandlers(ClientService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET:
          serviceImpl.get((ClientProto.GetRequest) request,
              (io.grpc.stub.StreamObserver<ClientProto.GetResponse>) responseObserver);
          break;
        case METHODID_PUT:
          serviceImpl.put((ClientProto.PutRequest) request,
              (io.grpc.stub.StreamObserver<ClientProto.PutResponse>) responseObserver);
          break;
        case METHODID_CHECK_AND_PUT:
          serviceImpl.checkAndPut((ClientProto.CheckAndPutRequest) request,
              (io.grpc.stub.StreamObserver<ClientProto.CheckAndPutResponse>) responseObserver);
          break;
        case METHODID_DELETE:
          serviceImpl.delete((ClientProto.DeleteRequest) request,
              (io.grpc.stub.StreamObserver<ClientProto.DeleteResponse>) responseObserver);
          break;
        case METHODID_BEGIN_TRANSACTION:
          serviceImpl.beginTransaction((ClientProto.BeginTransactionRequest) request,
              (io.grpc.stub.StreamObserver<ClientProto.BeginTransactionResponse>) responseObserver);
          break;
        case METHODID_ROLL_BACK:
          serviceImpl.rollBack((ClientProto.RollBackRequest) request,
              (io.grpc.stub.StreamObserver<ClientProto.RollBackResponse>) responseObserver);
          break;
        case METHODID_COMMIT:
          serviceImpl.commit((ClientProto.CommitRequest) request,
              (io.grpc.stub.StreamObserver<ClientProto.CommitResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static io.grpc.ServerServiceDefinition bindService(
      final ClientService serviceImpl) {
    return io.grpc.ServerServiceDefinition.builder(SERVICE_NAME)
        .addMethod(
          METHOD_GET,
          asyncUnaryCall(
            new MethodHandlers<
              ClientProto.GetRequest,
              ClientProto.GetResponse>(
                serviceImpl, METHODID_GET)))
        .addMethod(
          METHOD_PUT,
          asyncUnaryCall(
            new MethodHandlers<
              ClientProto.PutRequest,
              ClientProto.PutResponse>(
                serviceImpl, METHODID_PUT)))
        .addMethod(
          METHOD_CHECK_AND_PUT,
          asyncUnaryCall(
            new MethodHandlers<
              ClientProto.CheckAndPutRequest,
              ClientProto.CheckAndPutResponse>(
                serviceImpl, METHODID_CHECK_AND_PUT)))
        .addMethod(
          METHOD_DELETE,
          asyncUnaryCall(
            new MethodHandlers<
              ClientProto.DeleteRequest,
              ClientProto.DeleteResponse>(
                serviceImpl, METHODID_DELETE)))
        .addMethod(
          METHOD_BEGIN_TRANSACTION,
          asyncUnaryCall(
            new MethodHandlers<
              ClientProto.BeginTransactionRequest,
              ClientProto.BeginTransactionResponse>(
                serviceImpl, METHODID_BEGIN_TRANSACTION)))
        .addMethod(
          METHOD_ROLL_BACK,
          asyncUnaryCall(
            new MethodHandlers<
              ClientProto.RollBackRequest,
              ClientProto.RollBackResponse>(
                serviceImpl, METHODID_ROLL_BACK)))
        .addMethod(
          METHOD_COMMIT,
          asyncUnaryCall(
            new MethodHandlers<
              ClientProto.CommitRequest,
              ClientProto.CommitResponse>(
                serviceImpl, METHODID_COMMIT)))
        .build();
  }
}
