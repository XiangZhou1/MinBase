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

    private AdminServiceGrpc() {
    }

    public static final String SERVICE_NAME = "org.minbase.common.rpc.proto.generated.AdminService";

    // Static method descriptors that strictly reflect the proto.
    @io.grpc.ExperimentalApi
    public static final io.grpc.MethodDescriptor<AdminProto.CreateTableRequest,
            AdminProto.CreateTableResponse> METHOD_CREATE_TABLE =
            io.grpc.MethodDescriptor.create(
                    io.grpc.MethodDescriptor.MethodType.UNARY,
                    generateFullMethodName(
                            "org.minbase.common.rpc.proto.generated.AdminService", "createTable"),
                    io.grpc.protobuf.ProtoUtils.marshaller(AdminProto.CreateTableRequest.getDefaultInstance()),
                    io.grpc.protobuf.ProtoUtils.marshaller(AdminProto.CreateTableResponse.getDefaultInstance()));
    @io.grpc.ExperimentalApi
    public static final io.grpc.MethodDescriptor<AdminProto.DropTableRequest,
            AdminProto.DropTableResponse> METHOD_DROP_TABLE =
            io.grpc.MethodDescriptor.create(
                    io.grpc.MethodDescriptor.MethodType.UNARY,
                    generateFullMethodName(
                            "org.minbase.common.rpc.proto.generated.AdminService", "dropTable"),
                    io.grpc.protobuf.ProtoUtils.marshaller(AdminProto.DropTableRequest.getDefaultInstance()),
                    io.grpc.protobuf.ProtoUtils.marshaller(AdminProto.DropTableResponse.getDefaultInstance()));
    @io.grpc.ExperimentalApi
    public static final io.grpc.MethodDescriptor<AdminProto.TruncateTableRequest,
            AdminProto.TruncateTableResponse> METHOD_TRUNCATE_TABLE =
            io.grpc.MethodDescriptor.create(
                    io.grpc.MethodDescriptor.MethodType.UNARY,
                    generateFullMethodName(
                            "org.minbase.common.rpc.proto.generated.AdminService", "truncateTable"),
                    io.grpc.protobuf.ProtoUtils.marshaller(AdminProto.TruncateTableRequest.getDefaultInstance()),
                    io.grpc.protobuf.ProtoUtils.marshaller(AdminProto.TruncateTableResponse.getDefaultInstance()));

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

        public void createTable(AdminProto.CreateTableRequest request,
                                io.grpc.stub.StreamObserver<AdminProto.CreateTableResponse> responseObserver);

        public void dropTable(AdminProto.DropTableRequest request,
                              io.grpc.stub.StreamObserver<AdminProto.DropTableResponse> responseObserver);

        public void truncateTable(AdminProto.TruncateTableRequest request,
                                  io.grpc.stub.StreamObserver<AdminProto.TruncateTableResponse> responseObserver);
    }

    public static interface AdminServiceBlockingClient {

        public AdminProto.CreateTableResponse createTable(AdminProto.CreateTableRequest request);

        public AdminProto.DropTableResponse dropTable(AdminProto.DropTableRequest request);

        public AdminProto.TruncateTableResponse truncateTable(AdminProto.TruncateTableRequest request);
    }

    public static interface AdminServiceFutureClient {

        public com.google.common.util.concurrent.ListenableFuture<AdminProto.CreateTableResponse> createTable(
                AdminProto.CreateTableRequest request);

        public com.google.common.util.concurrent.ListenableFuture<AdminProto.DropTableResponse> dropTable(
                AdminProto.DropTableRequest request);

        public com.google.common.util.concurrent.ListenableFuture<AdminProto.TruncateTableResponse> truncateTable(
                AdminProto.TruncateTableRequest request);
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

        @Override
        protected AdminServiceStub build(io.grpc.Channel channel,
                                         io.grpc.CallOptions callOptions) {
            return new AdminServiceStub(channel, callOptions);
        }

        @Override
        public void createTable(AdminProto.CreateTableRequest request,
                                io.grpc.stub.StreamObserver<AdminProto.CreateTableResponse> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_CREATE_TABLE, getCallOptions()), request, responseObserver);
        }

        @Override
        public void dropTable(AdminProto.DropTableRequest request,
                              io.grpc.stub.StreamObserver<AdminProto.DropTableResponse> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_DROP_TABLE, getCallOptions()), request, responseObserver);
        }

        @Override
        public void truncateTable(AdminProto.TruncateTableRequest request,
                                  io.grpc.stub.StreamObserver<AdminProto.TruncateTableResponse> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_TRUNCATE_TABLE, getCallOptions()), request, responseObserver);
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

        @Override
        protected AdminServiceBlockingStub build(io.grpc.Channel channel,
                                                 io.grpc.CallOptions callOptions) {
            return new AdminServiceBlockingStub(channel, callOptions);
        }

        @Override
        public AdminProto.CreateTableResponse createTable(AdminProto.CreateTableRequest request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_CREATE_TABLE, getCallOptions(), request);
        }

        @Override
        public AdminProto.DropTableResponse dropTable(AdminProto.DropTableRequest request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_DROP_TABLE, getCallOptions(), request);
        }

        @Override
        public AdminProto.TruncateTableResponse truncateTable(AdminProto.TruncateTableRequest request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_TRUNCATE_TABLE, getCallOptions(), request);
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

        @Override
        protected AdminServiceFutureStub build(io.grpc.Channel channel,
                                               io.grpc.CallOptions callOptions) {
            return new AdminServiceFutureStub(channel, callOptions);
        }

        @Override
        public com.google.common.util.concurrent.ListenableFuture<AdminProto.CreateTableResponse> createTable(
                AdminProto.CreateTableRequest request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_CREATE_TABLE, getCallOptions()), request);
        }

        @Override
        public com.google.common.util.concurrent.ListenableFuture<AdminProto.DropTableResponse> dropTable(
                AdminProto.DropTableRequest request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_DROP_TABLE, getCallOptions()), request);
        }

        @Override
        public com.google.common.util.concurrent.ListenableFuture<AdminProto.TruncateTableResponse> truncateTable(
                AdminProto.TruncateTableRequest request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_TRUNCATE_TABLE, getCallOptions()), request);
        }
    }

    private static final int METHODID_CREATE_TABLE = 0;
    private static final int METHODID_DROP_TABLE = 1;
    private static final int METHODID_TRUNCATE_TABLE = 2;

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

        @SuppressWarnings("unchecked")
        public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
            switch (methodId) {
                case METHODID_CREATE_TABLE:
                    serviceImpl.createTable((AdminProto.CreateTableRequest) request,
                            (io.grpc.stub.StreamObserver<AdminProto.CreateTableResponse>) responseObserver);
                    break;
                case METHODID_DROP_TABLE:
                    serviceImpl.dropTable((AdminProto.DropTableRequest) request,
                            (io.grpc.stub.StreamObserver<AdminProto.DropTableResponse>) responseObserver);
                    break;
                case METHODID_TRUNCATE_TABLE:
                    serviceImpl.truncateTable((AdminProto.TruncateTableRequest) request,
                            (io.grpc.stub.StreamObserver<AdminProto.TruncateTableResponse>) responseObserver);
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
            final AdminService serviceImpl) {
        return io.grpc.ServerServiceDefinition.builder(SERVICE_NAME)
                .addMethod(
                        METHOD_CREATE_TABLE,
                        asyncUnaryCall(
                                new MethodHandlers<
                                        AdminProto.CreateTableRequest,
                                        AdminProto.CreateTableResponse>(
                                        serviceImpl, METHODID_CREATE_TABLE)))
                .addMethod(
                        METHOD_DROP_TABLE,
                        asyncUnaryCall(
                                new MethodHandlers<
                                        AdminProto.DropTableRequest,
                                        AdminProto.DropTableResponse>(
                                        serviceImpl, METHODID_DROP_TABLE)))
                .addMethod(
                        METHOD_TRUNCATE_TABLE,
                        asyncUnaryCall(
                                new MethodHandlers<
                                        AdminProto.TruncateTableRequest,
                                        AdminProto.TruncateTableResponse>(
                                        serviceImpl, METHODID_TRUNCATE_TABLE)))
                .build();
    }
}
