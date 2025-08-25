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
  public static final io.grpc.MethodDescriptor<org.minbase.common.rpc.proto.generated.ClientProto.GetRequest,
      org.minbase.common.rpc.proto.generated.ClientProto.GetResponse> METHOD_GET =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.ClientService", "get"),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.ClientProto.GetRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.ClientProto.GetResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<org.minbase.common.rpc.proto.generated.ClientProto.PutRequest,
      org.minbase.common.rpc.proto.generated.ClientProto.PutResponse> METHOD_PUT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.ClientService", "put"),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.ClientProto.PutRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.ClientProto.PutResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<org.minbase.common.rpc.proto.generated.ClientProto.CheckAndPutRequest,
      org.minbase.common.rpc.proto.generated.ClientProto.CheckAndPutResponse> METHOD_CHECK_AND_PUT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.ClientService", "checkAndPut"),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.ClientProto.CheckAndPutRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.ClientProto.CheckAndPutResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<org.minbase.common.rpc.proto.generated.ClientProto.DeleteRequest,
      org.minbase.common.rpc.proto.generated.ClientProto.DeleteResponse> METHOD_DELETE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.ClientService", "delete"),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.ClientProto.DeleteRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.ClientProto.DeleteResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<org.minbase.common.rpc.proto.generated.ClientProto.ScanRequest,
      org.minbase.common.rpc.proto.generated.ClientProto.ScanResponse> METHOD_SCAN =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.ClientService", "scan"),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.ClientProto.ScanRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.ClientProto.ScanResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<org.minbase.common.rpc.proto.generated.ClientProto.BeginTransactionRequest,
      org.minbase.common.rpc.proto.generated.ClientProto.BeginTransactionResponse> METHOD_BEGIN_TRANSACTION =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.ClientService", "beginTransaction"),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.ClientProto.BeginTransactionRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.ClientProto.BeginTransactionResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<org.minbase.common.rpc.proto.generated.ClientProto.RollBackRequest,
      org.minbase.common.rpc.proto.generated.ClientProto.RollBackResponse> METHOD_ROLL_BACK =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.ClientService", "rollBack"),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.ClientProto.RollBackRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.ClientProto.RollBackResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<org.minbase.common.rpc.proto.generated.ClientProto.CommitRequest,
      org.minbase.common.rpc.proto.generated.ClientProto.CommitResponse> METHOD_COMMIT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.ClientService", "commit"),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.ClientProto.CommitRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.ClientProto.CommitResponse.getDefaultInstance()));

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

    public void get(org.minbase.common.rpc.proto.generated.ClientProto.GetRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.GetResponse> responseObserver);

    public void put(org.minbase.common.rpc.proto.generated.ClientProto.PutRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.PutResponse> responseObserver);

    public void checkAndPut(org.minbase.common.rpc.proto.generated.ClientProto.CheckAndPutRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.CheckAndPutResponse> responseObserver);

    public void delete(org.minbase.common.rpc.proto.generated.ClientProto.DeleteRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.DeleteResponse> responseObserver);

    public void scan(org.minbase.common.rpc.proto.generated.ClientProto.ScanRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.ScanResponse> responseObserver);

    public void beginTransaction(org.minbase.common.rpc.proto.generated.ClientProto.BeginTransactionRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.BeginTransactionResponse> responseObserver);

    public void rollBack(org.minbase.common.rpc.proto.generated.ClientProto.RollBackRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.RollBackResponse> responseObserver);

    public void commit(org.minbase.common.rpc.proto.generated.ClientProto.CommitRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.CommitResponse> responseObserver);
  }

  public static interface ClientServiceBlockingClient {

    public org.minbase.common.rpc.proto.generated.ClientProto.GetResponse get(org.minbase.common.rpc.proto.generated.ClientProto.GetRequest request);

    public org.minbase.common.rpc.proto.generated.ClientProto.PutResponse put(org.minbase.common.rpc.proto.generated.ClientProto.PutRequest request);

    public org.minbase.common.rpc.proto.generated.ClientProto.CheckAndPutResponse checkAndPut(org.minbase.common.rpc.proto.generated.ClientProto.CheckAndPutRequest request);

    public org.minbase.common.rpc.proto.generated.ClientProto.DeleteResponse delete(org.minbase.common.rpc.proto.generated.ClientProto.DeleteRequest request);

    public org.minbase.common.rpc.proto.generated.ClientProto.ScanResponse scan(org.minbase.common.rpc.proto.generated.ClientProto.ScanRequest request);

    public org.minbase.common.rpc.proto.generated.ClientProto.BeginTransactionResponse beginTransaction(org.minbase.common.rpc.proto.generated.ClientProto.BeginTransactionRequest request);

    public org.minbase.common.rpc.proto.generated.ClientProto.RollBackResponse rollBack(org.minbase.common.rpc.proto.generated.ClientProto.RollBackRequest request);

    public org.minbase.common.rpc.proto.generated.ClientProto.CommitResponse commit(org.minbase.common.rpc.proto.generated.ClientProto.CommitRequest request);
  }

  public static interface ClientServiceFutureClient {

    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.ClientProto.GetResponse> get(
        org.minbase.common.rpc.proto.generated.ClientProto.GetRequest request);

    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.ClientProto.PutResponse> put(
        org.minbase.common.rpc.proto.generated.ClientProto.PutRequest request);

    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.ClientProto.CheckAndPutResponse> checkAndPut(
        org.minbase.common.rpc.proto.generated.ClientProto.CheckAndPutRequest request);

    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.ClientProto.DeleteResponse> delete(
        org.minbase.common.rpc.proto.generated.ClientProto.DeleteRequest request);

    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.ClientProto.ScanResponse> scan(
        org.minbase.common.rpc.proto.generated.ClientProto.ScanRequest request);

    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.ClientProto.BeginTransactionResponse> beginTransaction(
        org.minbase.common.rpc.proto.generated.ClientProto.BeginTransactionRequest request);

    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.ClientProto.RollBackResponse> rollBack(
        org.minbase.common.rpc.proto.generated.ClientProto.RollBackRequest request);

    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.ClientProto.CommitResponse> commit(
        org.minbase.common.rpc.proto.generated.ClientProto.CommitRequest request);
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

    @java.lang.Override
    protected ClientServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ClientServiceStub(channel, callOptions);
    }

    @java.lang.Override
    public void get(org.minbase.common.rpc.proto.generated.ClientProto.GetRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.GetResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void put(org.minbase.common.rpc.proto.generated.ClientProto.PutRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.PutResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_PUT, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void checkAndPut(org.minbase.common.rpc.proto.generated.ClientProto.CheckAndPutRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.CheckAndPutResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CHECK_AND_PUT, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void delete(org.minbase.common.rpc.proto.generated.ClientProto.DeleteRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.DeleteResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DELETE, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void scan(org.minbase.common.rpc.proto.generated.ClientProto.ScanRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.ScanResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_SCAN, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void beginTransaction(org.minbase.common.rpc.proto.generated.ClientProto.BeginTransactionRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.BeginTransactionResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_BEGIN_TRANSACTION, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void rollBack(org.minbase.common.rpc.proto.generated.ClientProto.RollBackRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.RollBackResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_ROLL_BACK, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void commit(org.minbase.common.rpc.proto.generated.ClientProto.CommitRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.CommitResponse> responseObserver) {
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

    @java.lang.Override
    protected ClientServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ClientServiceBlockingStub(channel, callOptions);
    }

    @java.lang.Override
    public org.minbase.common.rpc.proto.generated.ClientProto.GetResponse get(org.minbase.common.rpc.proto.generated.ClientProto.GetRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET, getCallOptions(), request);
    }

    @java.lang.Override
    public org.minbase.common.rpc.proto.generated.ClientProto.PutResponse put(org.minbase.common.rpc.proto.generated.ClientProto.PutRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_PUT, getCallOptions(), request);
    }

    @java.lang.Override
    public org.minbase.common.rpc.proto.generated.ClientProto.CheckAndPutResponse checkAndPut(org.minbase.common.rpc.proto.generated.ClientProto.CheckAndPutRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CHECK_AND_PUT, getCallOptions(), request);
    }

    @java.lang.Override
    public org.minbase.common.rpc.proto.generated.ClientProto.DeleteResponse delete(org.minbase.common.rpc.proto.generated.ClientProto.DeleteRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DELETE, getCallOptions(), request);
    }

    @java.lang.Override
    public org.minbase.common.rpc.proto.generated.ClientProto.ScanResponse scan(org.minbase.common.rpc.proto.generated.ClientProto.ScanRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_SCAN, getCallOptions(), request);
    }

    @java.lang.Override
    public org.minbase.common.rpc.proto.generated.ClientProto.BeginTransactionResponse beginTransaction(org.minbase.common.rpc.proto.generated.ClientProto.BeginTransactionRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_BEGIN_TRANSACTION, getCallOptions(), request);
    }

    @java.lang.Override
    public org.minbase.common.rpc.proto.generated.ClientProto.RollBackResponse rollBack(org.minbase.common.rpc.proto.generated.ClientProto.RollBackRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_ROLL_BACK, getCallOptions(), request);
    }

    @java.lang.Override
    public org.minbase.common.rpc.proto.generated.ClientProto.CommitResponse commit(org.minbase.common.rpc.proto.generated.ClientProto.CommitRequest request) {
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

    @java.lang.Override
    protected ClientServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ClientServiceFutureStub(channel, callOptions);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.ClientProto.GetResponse> get(
        org.minbase.common.rpc.proto.generated.ClientProto.GetRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.ClientProto.PutResponse> put(
        org.minbase.common.rpc.proto.generated.ClientProto.PutRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_PUT, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.ClientProto.CheckAndPutResponse> checkAndPut(
        org.minbase.common.rpc.proto.generated.ClientProto.CheckAndPutRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CHECK_AND_PUT, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.ClientProto.DeleteResponse> delete(
        org.minbase.common.rpc.proto.generated.ClientProto.DeleteRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DELETE, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.ClientProto.ScanResponse> scan(
        org.minbase.common.rpc.proto.generated.ClientProto.ScanRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_SCAN, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.ClientProto.BeginTransactionResponse> beginTransaction(
        org.minbase.common.rpc.proto.generated.ClientProto.BeginTransactionRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_BEGIN_TRANSACTION, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.ClientProto.RollBackResponse> rollBack(
        org.minbase.common.rpc.proto.generated.ClientProto.RollBackRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_ROLL_BACK, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.ClientProto.CommitResponse> commit(
        org.minbase.common.rpc.proto.generated.ClientProto.CommitRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_COMMIT, getCallOptions()), request);
    }
  }

  private static final int METHODID_GET = 0;
  private static final int METHODID_PUT = 1;
  private static final int METHODID_CHECK_AND_PUT = 2;
  private static final int METHODID_DELETE = 3;
  private static final int METHODID_SCAN = 4;
  private static final int METHODID_BEGIN_TRANSACTION = 5;
  private static final int METHODID_ROLL_BACK = 6;
  private static final int METHODID_COMMIT = 7;

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

    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET:
          serviceImpl.get((org.minbase.common.rpc.proto.generated.ClientProto.GetRequest) request,
              (io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.GetResponse>) responseObserver);
          break;
        case METHODID_PUT:
          serviceImpl.put((org.minbase.common.rpc.proto.generated.ClientProto.PutRequest) request,
              (io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.PutResponse>) responseObserver);
          break;
        case METHODID_CHECK_AND_PUT:
          serviceImpl.checkAndPut((org.minbase.common.rpc.proto.generated.ClientProto.CheckAndPutRequest) request,
              (io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.CheckAndPutResponse>) responseObserver);
          break;
        case METHODID_DELETE:
          serviceImpl.delete((org.minbase.common.rpc.proto.generated.ClientProto.DeleteRequest) request,
              (io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.DeleteResponse>) responseObserver);
          break;
        case METHODID_SCAN:
          serviceImpl.scan((org.minbase.common.rpc.proto.generated.ClientProto.ScanRequest) request,
              (io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.ScanResponse>) responseObserver);
          break;
        case METHODID_BEGIN_TRANSACTION:
          serviceImpl.beginTransaction((org.minbase.common.rpc.proto.generated.ClientProto.BeginTransactionRequest) request,
              (io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.BeginTransactionResponse>) responseObserver);
          break;
        case METHODID_ROLL_BACK:
          serviceImpl.rollBack((org.minbase.common.rpc.proto.generated.ClientProto.RollBackRequest) request,
              (io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.RollBackResponse>) responseObserver);
          break;
        case METHODID_COMMIT:
          serviceImpl.commit((org.minbase.common.rpc.proto.generated.ClientProto.CommitRequest) request,
              (io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.CommitResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.SuppressWarnings("unchecked")
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
              org.minbase.common.rpc.proto.generated.ClientProto.GetRequest,
              org.minbase.common.rpc.proto.generated.ClientProto.GetResponse>(
                serviceImpl, METHODID_GET)))
        .addMethod(
          METHOD_PUT,
          asyncUnaryCall(
            new MethodHandlers<
              org.minbase.common.rpc.proto.generated.ClientProto.PutRequest,
              org.minbase.common.rpc.proto.generated.ClientProto.PutResponse>(
                serviceImpl, METHODID_PUT)))
        .addMethod(
          METHOD_CHECK_AND_PUT,
          asyncUnaryCall(
            new MethodHandlers<
              org.minbase.common.rpc.proto.generated.ClientProto.CheckAndPutRequest,
              org.minbase.common.rpc.proto.generated.ClientProto.CheckAndPutResponse>(
                serviceImpl, METHODID_CHECK_AND_PUT)))
        .addMethod(
          METHOD_DELETE,
          asyncUnaryCall(
            new MethodHandlers<
              org.minbase.common.rpc.proto.generated.ClientProto.DeleteRequest,
              org.minbase.common.rpc.proto.generated.ClientProto.DeleteResponse>(
                serviceImpl, METHODID_DELETE)))
        .addMethod(
          METHOD_SCAN,
          asyncUnaryCall(
            new MethodHandlers<
              org.minbase.common.rpc.proto.generated.ClientProto.ScanRequest,
              org.minbase.common.rpc.proto.generated.ClientProto.ScanResponse>(
                serviceImpl, METHODID_SCAN)))
        .addMethod(
          METHOD_BEGIN_TRANSACTION,
          asyncUnaryCall(
            new MethodHandlers<
              org.minbase.common.rpc.proto.generated.ClientProto.BeginTransactionRequest,
              org.minbase.common.rpc.proto.generated.ClientProto.BeginTransactionResponse>(
                serviceImpl, METHODID_BEGIN_TRANSACTION)))
        .addMethod(
          METHOD_ROLL_BACK,
          asyncUnaryCall(
            new MethodHandlers<
              org.minbase.common.rpc.proto.generated.ClientProto.RollBackRequest,
              org.minbase.common.rpc.proto.generated.ClientProto.RollBackResponse>(
                serviceImpl, METHODID_ROLL_BACK)))
        .addMethod(
          METHOD_COMMIT,
          asyncUnaryCall(
            new MethodHandlers<
              org.minbase.common.rpc.proto.generated.ClientProto.CommitRequest,
              org.minbase.common.rpc.proto.generated.ClientProto.CommitResponse>(
                serviceImpl, METHODID_COMMIT)))
        .build();
  }
}
