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
public class TransactionServiceGrpc {

  private TransactionServiceGrpc() {}

  public static final String SERVICE_NAME = "org.minbase.common.rpc.proto.generated.TransactionService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<ClientProto.TxGetRequest,
      ClientProto.TxGetResponse> METHOD_GET =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.TransactionService", "get"),
          io.grpc.protobuf.ProtoUtils.marshaller(ClientProto.TxGetRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(ClientProto.TxGetResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<ClientProto.TxPutRequest,
      ClientProto.TxPutResponse> METHOD_PUT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.TransactionService", "put"),
          io.grpc.protobuf.ProtoUtils.marshaller(ClientProto.TxPutRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(ClientProto.TxPutResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<ClientProto.TxCheckAndPutRequest,
      ClientProto.TxCheckAndPutResponse> METHOD_CHECK_AND_PUT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.TransactionService", "checkAndPut"),
          io.grpc.protobuf.ProtoUtils.marshaller(ClientProto.TxCheckAndPutRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(ClientProto.TxCheckAndPutResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<ClientProto.TxDeleteRequest,
      ClientProto.TxDeleteResponse> METHOD_DELETE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.TransactionService", "delete"),
          io.grpc.protobuf.ProtoUtils.marshaller(ClientProto.TxDeleteRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(ClientProto.TxDeleteResponse.getDefaultInstance()));

  public static TransactionServiceStub newStub(io.grpc.Channel channel) {
    return new TransactionServiceStub(channel);
  }

  public static TransactionServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new TransactionServiceBlockingStub(channel);
  }

  public static TransactionServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new TransactionServiceFutureStub(channel);
  }

  public static interface TransactionService {

    public void get(ClientProto.TxGetRequest request,
                    io.grpc.stub.StreamObserver<ClientProto.TxGetResponse> responseObserver);

    public void put(ClientProto.TxPutRequest request,
                    io.grpc.stub.StreamObserver<ClientProto.TxPutResponse> responseObserver);

    public void checkAndPut(ClientProto.TxCheckAndPutRequest request,
                            io.grpc.stub.StreamObserver<ClientProto.TxCheckAndPutResponse> responseObserver);

    public void delete(ClientProto.TxDeleteRequest request,
                       io.grpc.stub.StreamObserver<ClientProto.TxDeleteResponse> responseObserver);
  }

  public static interface TransactionServiceBlockingClient {

    public ClientProto.TxGetResponse get(ClientProto.TxGetRequest request);

    public ClientProto.TxPutResponse put(ClientProto.TxPutRequest request);

    public ClientProto.TxCheckAndPutResponse checkAndPut(ClientProto.TxCheckAndPutRequest request);

    public ClientProto.TxDeleteResponse delete(ClientProto.TxDeleteRequest request);
  }

  public static interface TransactionServiceFutureClient {

    public com.google.common.util.concurrent.ListenableFuture<ClientProto.TxGetResponse> get(
        ClientProto.TxGetRequest request);

    public com.google.common.util.concurrent.ListenableFuture<ClientProto.TxPutResponse> put(
        ClientProto.TxPutRequest request);

    public com.google.common.util.concurrent.ListenableFuture<ClientProto.TxCheckAndPutResponse> checkAndPut(
        ClientProto.TxCheckAndPutRequest request);

    public com.google.common.util.concurrent.ListenableFuture<ClientProto.TxDeleteResponse> delete(
        ClientProto.TxDeleteRequest request);
  }

  public static class TransactionServiceStub extends io.grpc.stub.AbstractStub<TransactionServiceStub>
      implements TransactionService {
    private TransactionServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private TransactionServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected TransactionServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new TransactionServiceStub(channel, callOptions);
    }

    @Override
    public void get(ClientProto.TxGetRequest request,
                    io.grpc.stub.StreamObserver<ClientProto.TxGetResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET, getCallOptions()), request, responseObserver);
    }

    @Override
    public void put(ClientProto.TxPutRequest request,
                    io.grpc.stub.StreamObserver<ClientProto.TxPutResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_PUT, getCallOptions()), request, responseObserver);
    }

    @Override
    public void checkAndPut(ClientProto.TxCheckAndPutRequest request,
                            io.grpc.stub.StreamObserver<ClientProto.TxCheckAndPutResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CHECK_AND_PUT, getCallOptions()), request, responseObserver);
    }

    @Override
    public void delete(ClientProto.TxDeleteRequest request,
                       io.grpc.stub.StreamObserver<ClientProto.TxDeleteResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DELETE, getCallOptions()), request, responseObserver);
    }
  }

  public static class TransactionServiceBlockingStub extends io.grpc.stub.AbstractStub<TransactionServiceBlockingStub>
      implements TransactionServiceBlockingClient {
    private TransactionServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private TransactionServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected TransactionServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new TransactionServiceBlockingStub(channel, callOptions);
    }

    @Override
    public ClientProto.TxGetResponse get(ClientProto.TxGetRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET, getCallOptions(), request);
    }

    @Override
    public ClientProto.TxPutResponse put(ClientProto.TxPutRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_PUT, getCallOptions(), request);
    }

    @Override
    public ClientProto.TxCheckAndPutResponse checkAndPut(ClientProto.TxCheckAndPutRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CHECK_AND_PUT, getCallOptions(), request);
    }

    @Override
    public ClientProto.TxDeleteResponse delete(ClientProto.TxDeleteRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DELETE, getCallOptions(), request);
    }
  }

  public static class TransactionServiceFutureStub extends io.grpc.stub.AbstractStub<TransactionServiceFutureStub>
      implements TransactionServiceFutureClient {
    private TransactionServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private TransactionServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected TransactionServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new TransactionServiceFutureStub(channel, callOptions);
    }

    @Override
    public com.google.common.util.concurrent.ListenableFuture<ClientProto.TxGetResponse> get(
        ClientProto.TxGetRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET, getCallOptions()), request);
    }

    @Override
    public com.google.common.util.concurrent.ListenableFuture<ClientProto.TxPutResponse> put(
        ClientProto.TxPutRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_PUT, getCallOptions()), request);
    }

    @Override
    public com.google.common.util.concurrent.ListenableFuture<ClientProto.TxCheckAndPutResponse> checkAndPut(
        ClientProto.TxCheckAndPutRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CHECK_AND_PUT, getCallOptions()), request);
    }

    @Override
    public com.google.common.util.concurrent.ListenableFuture<ClientProto.TxDeleteResponse> delete(
        ClientProto.TxDeleteRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DELETE, getCallOptions()), request);
    }
  }

  private static final int METHODID_GET = 0;
  private static final int METHODID_PUT = 1;
  private static final int METHODID_CHECK_AND_PUT = 2;
  private static final int METHODID_DELETE = 3;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final TransactionService serviceImpl;
    private final int methodId;

    public MethodHandlers(TransactionService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET:
          serviceImpl.get((ClientProto.TxGetRequest) request,
              (io.grpc.stub.StreamObserver<ClientProto.TxGetResponse>) responseObserver);
          break;
        case METHODID_PUT:
          serviceImpl.put((ClientProto.TxPutRequest) request,
              (io.grpc.stub.StreamObserver<ClientProto.TxPutResponse>) responseObserver);
          break;
        case METHODID_CHECK_AND_PUT:
          serviceImpl.checkAndPut((ClientProto.TxCheckAndPutRequest) request,
              (io.grpc.stub.StreamObserver<ClientProto.TxCheckAndPutResponse>) responseObserver);
          break;
        case METHODID_DELETE:
          serviceImpl.delete((ClientProto.TxDeleteRequest) request,
              (io.grpc.stub.StreamObserver<ClientProto.TxDeleteResponse>) responseObserver);
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
      final TransactionService serviceImpl) {
    return io.grpc.ServerServiceDefinition.builder(SERVICE_NAME)
        .addMethod(
          METHOD_GET,
          asyncUnaryCall(
            new MethodHandlers<
              ClientProto.TxGetRequest,
              ClientProto.TxGetResponse>(
                serviceImpl, METHODID_GET)))
        .addMethod(
          METHOD_PUT,
          asyncUnaryCall(
            new MethodHandlers<
              ClientProto.TxPutRequest,
              ClientProto.TxPutResponse>(
                serviceImpl, METHODID_PUT)))
        .addMethod(
          METHOD_CHECK_AND_PUT,
          asyncUnaryCall(
            new MethodHandlers<
              ClientProto.TxCheckAndPutRequest,
              ClientProto.TxCheckAndPutResponse>(
                serviceImpl, METHODID_CHECK_AND_PUT)))
        .addMethod(
          METHOD_DELETE,
          asyncUnaryCall(
            new MethodHandlers<
              ClientProto.TxDeleteRequest,
              ClientProto.TxDeleteResponse>(
                serviceImpl, METHODID_DELETE)))
        .build();
  }
}
