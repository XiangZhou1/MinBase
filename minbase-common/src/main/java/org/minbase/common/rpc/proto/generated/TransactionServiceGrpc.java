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
  public static final io.grpc.MethodDescriptor<org.minbase.common.rpc.proto.generated.ClientProto.TxGetRequest,
      org.minbase.common.rpc.proto.generated.ClientProto.TxGetResponse> METHOD_GET =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.TransactionService", "get"),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.ClientProto.TxGetRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.ClientProto.TxGetResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<org.minbase.common.rpc.proto.generated.ClientProto.TxPutRequest,
      org.minbase.common.rpc.proto.generated.ClientProto.TxPutResponse> METHOD_PUT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.TransactionService", "put"),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.ClientProto.TxPutRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.ClientProto.TxPutResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<org.minbase.common.rpc.proto.generated.ClientProto.TxCheckAndPutRequest,
      org.minbase.common.rpc.proto.generated.ClientProto.TxCheckAndPutResponse> METHOD_CHECK_AND_PUT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.TransactionService", "checkAndPut"),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.ClientProto.TxCheckAndPutRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.ClientProto.TxCheckAndPutResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<org.minbase.common.rpc.proto.generated.ClientProto.TxDeleteRequest,
      org.minbase.common.rpc.proto.generated.ClientProto.TxDeleteResponse> METHOD_DELETE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.TransactionService", "delete"),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.ClientProto.TxDeleteRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.ClientProto.TxDeleteResponse.getDefaultInstance()));

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

    public void get(org.minbase.common.rpc.proto.generated.ClientProto.TxGetRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.TxGetResponse> responseObserver);

    public void put(org.minbase.common.rpc.proto.generated.ClientProto.TxPutRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.TxPutResponse> responseObserver);

    public void checkAndPut(org.minbase.common.rpc.proto.generated.ClientProto.TxCheckAndPutRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.TxCheckAndPutResponse> responseObserver);

    public void delete(org.minbase.common.rpc.proto.generated.ClientProto.TxDeleteRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.TxDeleteResponse> responseObserver);
  }

  public static interface TransactionServiceBlockingClient {

    public org.minbase.common.rpc.proto.generated.ClientProto.TxGetResponse get(org.minbase.common.rpc.proto.generated.ClientProto.TxGetRequest request);

    public org.minbase.common.rpc.proto.generated.ClientProto.TxPutResponse put(org.minbase.common.rpc.proto.generated.ClientProto.TxPutRequest request);

    public org.minbase.common.rpc.proto.generated.ClientProto.TxCheckAndPutResponse checkAndPut(org.minbase.common.rpc.proto.generated.ClientProto.TxCheckAndPutRequest request);

    public org.minbase.common.rpc.proto.generated.ClientProto.TxDeleteResponse delete(org.minbase.common.rpc.proto.generated.ClientProto.TxDeleteRequest request);
  }

  public static interface TransactionServiceFutureClient {

    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.ClientProto.TxGetResponse> get(
        org.minbase.common.rpc.proto.generated.ClientProto.TxGetRequest request);

    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.ClientProto.TxPutResponse> put(
        org.minbase.common.rpc.proto.generated.ClientProto.TxPutRequest request);

    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.ClientProto.TxCheckAndPutResponse> checkAndPut(
        org.minbase.common.rpc.proto.generated.ClientProto.TxCheckAndPutRequest request);

    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.ClientProto.TxDeleteResponse> delete(
        org.minbase.common.rpc.proto.generated.ClientProto.TxDeleteRequest request);
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

    @java.lang.Override
    protected TransactionServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new TransactionServiceStub(channel, callOptions);
    }

    @java.lang.Override
    public void get(org.minbase.common.rpc.proto.generated.ClientProto.TxGetRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.TxGetResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void put(org.minbase.common.rpc.proto.generated.ClientProto.TxPutRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.TxPutResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_PUT, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void checkAndPut(org.minbase.common.rpc.proto.generated.ClientProto.TxCheckAndPutRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.TxCheckAndPutResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CHECK_AND_PUT, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void delete(org.minbase.common.rpc.proto.generated.ClientProto.TxDeleteRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.TxDeleteResponse> responseObserver) {
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

    @java.lang.Override
    protected TransactionServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new TransactionServiceBlockingStub(channel, callOptions);
    }

    @java.lang.Override
    public org.minbase.common.rpc.proto.generated.ClientProto.TxGetResponse get(org.minbase.common.rpc.proto.generated.ClientProto.TxGetRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET, getCallOptions(), request);
    }

    @java.lang.Override
    public org.minbase.common.rpc.proto.generated.ClientProto.TxPutResponse put(org.minbase.common.rpc.proto.generated.ClientProto.TxPutRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_PUT, getCallOptions(), request);
    }

    @java.lang.Override
    public org.minbase.common.rpc.proto.generated.ClientProto.TxCheckAndPutResponse checkAndPut(org.minbase.common.rpc.proto.generated.ClientProto.TxCheckAndPutRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CHECK_AND_PUT, getCallOptions(), request);
    }

    @java.lang.Override
    public org.minbase.common.rpc.proto.generated.ClientProto.TxDeleteResponse delete(org.minbase.common.rpc.proto.generated.ClientProto.TxDeleteRequest request) {
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

    @java.lang.Override
    protected TransactionServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new TransactionServiceFutureStub(channel, callOptions);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.ClientProto.TxGetResponse> get(
        org.minbase.common.rpc.proto.generated.ClientProto.TxGetRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.ClientProto.TxPutResponse> put(
        org.minbase.common.rpc.proto.generated.ClientProto.TxPutRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_PUT, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.ClientProto.TxCheckAndPutResponse> checkAndPut(
        org.minbase.common.rpc.proto.generated.ClientProto.TxCheckAndPutRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CHECK_AND_PUT, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.ClientProto.TxDeleteResponse> delete(
        org.minbase.common.rpc.proto.generated.ClientProto.TxDeleteRequest request) {
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

    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET:
          serviceImpl.get((org.minbase.common.rpc.proto.generated.ClientProto.TxGetRequest) request,
              (io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.TxGetResponse>) responseObserver);
          break;
        case METHODID_PUT:
          serviceImpl.put((org.minbase.common.rpc.proto.generated.ClientProto.TxPutRequest) request,
              (io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.TxPutResponse>) responseObserver);
          break;
        case METHODID_CHECK_AND_PUT:
          serviceImpl.checkAndPut((org.minbase.common.rpc.proto.generated.ClientProto.TxCheckAndPutRequest) request,
              (io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.TxCheckAndPutResponse>) responseObserver);
          break;
        case METHODID_DELETE:
          serviceImpl.delete((org.minbase.common.rpc.proto.generated.ClientProto.TxDeleteRequest) request,
              (io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.ClientProto.TxDeleteResponse>) responseObserver);
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
      final TransactionService serviceImpl) {
    return io.grpc.ServerServiceDefinition.builder(SERVICE_NAME)
        .addMethod(
          METHOD_GET,
          asyncUnaryCall(
            new MethodHandlers<
              org.minbase.common.rpc.proto.generated.ClientProto.TxGetRequest,
              org.minbase.common.rpc.proto.generated.ClientProto.TxGetResponse>(
                serviceImpl, METHODID_GET)))
        .addMethod(
          METHOD_PUT,
          asyncUnaryCall(
            new MethodHandlers<
              org.minbase.common.rpc.proto.generated.ClientProto.TxPutRequest,
              org.minbase.common.rpc.proto.generated.ClientProto.TxPutResponse>(
                serviceImpl, METHODID_PUT)))
        .addMethod(
          METHOD_CHECK_AND_PUT,
          asyncUnaryCall(
            new MethodHandlers<
              org.minbase.common.rpc.proto.generated.ClientProto.TxCheckAndPutRequest,
              org.minbase.common.rpc.proto.generated.ClientProto.TxCheckAndPutResponse>(
                serviceImpl, METHODID_CHECK_AND_PUT)))
        .addMethod(
          METHOD_DELETE,
          asyncUnaryCall(
            new MethodHandlers<
              org.minbase.common.rpc.proto.generated.ClientProto.TxDeleteRequest,
              org.minbase.common.rpc.proto.generated.ClientProto.TxDeleteResponse>(
                serviceImpl, METHODID_DELETE)))
        .build();
  }
}
