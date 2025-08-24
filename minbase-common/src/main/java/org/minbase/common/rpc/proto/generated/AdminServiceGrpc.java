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
public class AdminServiceGrpc {

  private AdminServiceGrpc() {}

  public static final String SERVICE_NAME = "org.minbase.common.rpc.proto.generated.AdminService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<org.minbase.common.rpc.proto.generated.AdminProto.CreateTableRequest,
      org.minbase.common.rpc.proto.generated.AdminProto.CreateTableResponse> METHOD_CREATE_TABLE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.AdminService", "createTable"),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.AdminProto.CreateTableRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.AdminProto.CreateTableResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<org.minbase.common.rpc.proto.generated.AdminProto.DropTableRequest,
      org.minbase.common.rpc.proto.generated.AdminProto.DropTableResponse> METHOD_DROP_TABLE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.AdminService", "dropTable"),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.AdminProto.DropTableRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.AdminProto.DropTableResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<org.minbase.common.rpc.proto.generated.AdminProto.TruncateTableRequest,
      org.minbase.common.rpc.proto.generated.AdminProto.TruncateTableResponse> METHOD_TRUNCATE_TABLE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.AdminService", "truncateTable"),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.AdminProto.TruncateTableRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.AdminProto.TruncateTableResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<org.minbase.common.rpc.proto.generated.AdminProto.ListTablesRequest,
      org.minbase.common.rpc.proto.generated.AdminProto.ListTablesResponse> METHOD_LIST_TABLES =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "org.minbase.common.rpc.proto.generated.AdminService", "listTables"),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.AdminProto.ListTablesRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(org.minbase.common.rpc.proto.generated.AdminProto.ListTablesResponse.getDefaultInstance()));

  public static AdminServiceStub newStub(io.grpc.Channel channel) {
    return new AdminServiceStub(channel);
  }

  public static AdminServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new AdminServiceBlockingStub(channel);
  }

  public static AdminServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new AdminServiceFutureStub(channel);
  }

  public static interface AdminService {

    public void createTable(org.minbase.common.rpc.proto.generated.AdminProto.CreateTableRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.AdminProto.CreateTableResponse> responseObserver);

    public void dropTable(org.minbase.common.rpc.proto.generated.AdminProto.DropTableRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.AdminProto.DropTableResponse> responseObserver);

    public void truncateTable(org.minbase.common.rpc.proto.generated.AdminProto.TruncateTableRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.AdminProto.TruncateTableResponse> responseObserver);

    public void listTables(org.minbase.common.rpc.proto.generated.AdminProto.ListTablesRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.AdminProto.ListTablesResponse> responseObserver);
  }

  public static interface AdminServiceBlockingClient {

    public org.minbase.common.rpc.proto.generated.AdminProto.CreateTableResponse createTable(org.minbase.common.rpc.proto.generated.AdminProto.CreateTableRequest request);

    public org.minbase.common.rpc.proto.generated.AdminProto.DropTableResponse dropTable(org.minbase.common.rpc.proto.generated.AdminProto.DropTableRequest request);

    public org.minbase.common.rpc.proto.generated.AdminProto.TruncateTableResponse truncateTable(org.minbase.common.rpc.proto.generated.AdminProto.TruncateTableRequest request);

    public org.minbase.common.rpc.proto.generated.AdminProto.ListTablesResponse listTables(org.minbase.common.rpc.proto.generated.AdminProto.ListTablesRequest request);
  }

  public static interface AdminServiceFutureClient {

    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.AdminProto.CreateTableResponse> createTable(
        org.minbase.common.rpc.proto.generated.AdminProto.CreateTableRequest request);

    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.AdminProto.DropTableResponse> dropTable(
        org.minbase.common.rpc.proto.generated.AdminProto.DropTableRequest request);

    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.AdminProto.TruncateTableResponse> truncateTable(
        org.minbase.common.rpc.proto.generated.AdminProto.TruncateTableRequest request);

    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.AdminProto.ListTablesResponse> listTables(
        org.minbase.common.rpc.proto.generated.AdminProto.ListTablesRequest request);
  }

  public static class AdminServiceStub extends io.grpc.stub.AbstractStub<AdminServiceStub>
      implements AdminService {
    private AdminServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private AdminServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AdminServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new AdminServiceStub(channel, callOptions);
    }

    @java.lang.Override
    public void createTable(org.minbase.common.rpc.proto.generated.AdminProto.CreateTableRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.AdminProto.CreateTableResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CREATE_TABLE, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void dropTable(org.minbase.common.rpc.proto.generated.AdminProto.DropTableRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.AdminProto.DropTableResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DROP_TABLE, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void truncateTable(org.minbase.common.rpc.proto.generated.AdminProto.TruncateTableRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.AdminProto.TruncateTableResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_TRUNCATE_TABLE, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void listTables(org.minbase.common.rpc.proto.generated.AdminProto.ListTablesRequest request,
        io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.AdminProto.ListTablesResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_LIST_TABLES, getCallOptions()), request, responseObserver);
    }
  }

  public static class AdminServiceBlockingStub extends io.grpc.stub.AbstractStub<AdminServiceBlockingStub>
      implements AdminServiceBlockingClient {
    private AdminServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private AdminServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AdminServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new AdminServiceBlockingStub(channel, callOptions);
    }

    @java.lang.Override
    public org.minbase.common.rpc.proto.generated.AdminProto.CreateTableResponse createTable(org.minbase.common.rpc.proto.generated.AdminProto.CreateTableRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CREATE_TABLE, getCallOptions(), request);
    }

    @java.lang.Override
    public org.minbase.common.rpc.proto.generated.AdminProto.DropTableResponse dropTable(org.minbase.common.rpc.proto.generated.AdminProto.DropTableRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DROP_TABLE, getCallOptions(), request);
    }

    @java.lang.Override
    public org.minbase.common.rpc.proto.generated.AdminProto.TruncateTableResponse truncateTable(org.minbase.common.rpc.proto.generated.AdminProto.TruncateTableRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_TRUNCATE_TABLE, getCallOptions(), request);
    }

    @java.lang.Override
    public org.minbase.common.rpc.proto.generated.AdminProto.ListTablesResponse listTables(org.minbase.common.rpc.proto.generated.AdminProto.ListTablesRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_LIST_TABLES, getCallOptions(), request);
    }
  }

  public static class AdminServiceFutureStub extends io.grpc.stub.AbstractStub<AdminServiceFutureStub>
      implements AdminServiceFutureClient {
    private AdminServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private AdminServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AdminServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new AdminServiceFutureStub(channel, callOptions);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.AdminProto.CreateTableResponse> createTable(
        org.minbase.common.rpc.proto.generated.AdminProto.CreateTableRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CREATE_TABLE, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.AdminProto.DropTableResponse> dropTable(
        org.minbase.common.rpc.proto.generated.AdminProto.DropTableRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DROP_TABLE, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.AdminProto.TruncateTableResponse> truncateTable(
        org.minbase.common.rpc.proto.generated.AdminProto.TruncateTableRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_TRUNCATE_TABLE, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<org.minbase.common.rpc.proto.generated.AdminProto.ListTablesResponse> listTables(
        org.minbase.common.rpc.proto.generated.AdminProto.ListTablesRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_LIST_TABLES, getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE_TABLE = 0;
  private static final int METHODID_DROP_TABLE = 1;
  private static final int METHODID_TRUNCATE_TABLE = 2;
  private static final int METHODID_LIST_TABLES = 3;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AdminService serviceImpl;
    private final int methodId;

    public MethodHandlers(AdminService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CREATE_TABLE:
          serviceImpl.createTable((org.minbase.common.rpc.proto.generated.AdminProto.CreateTableRequest) request,
              (io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.AdminProto.CreateTableResponse>) responseObserver);
          break;
        case METHODID_DROP_TABLE:
          serviceImpl.dropTable((org.minbase.common.rpc.proto.generated.AdminProto.DropTableRequest) request,
              (io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.AdminProto.DropTableResponse>) responseObserver);
          break;
        case METHODID_TRUNCATE_TABLE:
          serviceImpl.truncateTable((org.minbase.common.rpc.proto.generated.AdminProto.TruncateTableRequest) request,
              (io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.AdminProto.TruncateTableResponse>) responseObserver);
          break;
        case METHODID_LIST_TABLES:
          serviceImpl.listTables((org.minbase.common.rpc.proto.generated.AdminProto.ListTablesRequest) request,
              (io.grpc.stub.StreamObserver<org.minbase.common.rpc.proto.generated.AdminProto.ListTablesResponse>) responseObserver);
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
      final AdminService serviceImpl) {
    return io.grpc.ServerServiceDefinition.builder(SERVICE_NAME)
        .addMethod(
          METHOD_CREATE_TABLE,
          asyncUnaryCall(
            new MethodHandlers<
              org.minbase.common.rpc.proto.generated.AdminProto.CreateTableRequest,
              org.minbase.common.rpc.proto.generated.AdminProto.CreateTableResponse>(
                serviceImpl, METHODID_CREATE_TABLE)))
        .addMethod(
          METHOD_DROP_TABLE,
          asyncUnaryCall(
            new MethodHandlers<
              org.minbase.common.rpc.proto.generated.AdminProto.DropTableRequest,
              org.minbase.common.rpc.proto.generated.AdminProto.DropTableResponse>(
                serviceImpl, METHODID_DROP_TABLE)))
        .addMethod(
          METHOD_TRUNCATE_TABLE,
          asyncUnaryCall(
            new MethodHandlers<
              org.minbase.common.rpc.proto.generated.AdminProto.TruncateTableRequest,
              org.minbase.common.rpc.proto.generated.AdminProto.TruncateTableResponse>(
                serviceImpl, METHODID_TRUNCATE_TABLE)))
        .addMethod(
          METHOD_LIST_TABLES,
          asyncUnaryCall(
            new MethodHandlers<
              org.minbase.common.rpc.proto.generated.AdminProto.ListTablesRequest,
              org.minbase.common.rpc.proto.generated.AdminProto.ListTablesResponse>(
                serviceImpl, METHODID_LIST_TABLES)))
        .build();
  }
}
