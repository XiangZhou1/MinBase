package org.minbase.client;


import com.google.protobuf.ByteString;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Promise;
import org.minbase.client.handler.MinClientHandler;
import org.minbase.client.service.AdminService;
import org.minbase.client.service.ClientService;
import org.minbase.client.service.TxService;
import org.minbase.client.table.ClientTableImpl;
import org.minbase.client.transaction.ClientTransaction;
import org.minbase.common.exception.TransactionNotExistException;
import org.minbase.common.rpc.codec.RpcFrameDecoder;
import org.minbase.common.rpc.codec.RpcRequestEncoder;
import org.minbase.common.rpc.codec.RpcResponseDecoder;
import org.minbase.common.rpc.proto.generated.AdminProto;
import org.minbase.common.rpc.proto.generated.ClientProto;
import org.minbase.common.rpc.proto.generated.RpcProto;
import org.minbase.common.rpc.service.StatusCode;
import org.minbase.common.table.ClientTable;
import org.minbase.common.table.TableInfo;
import org.minbase.common.table.transaction.Transaction;
import org.minbase.common.utils.ProtobufUtil;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MinClient {
    private final String host;
    private final int port;
    private Channel channel;
    private AtomicLong requestId;
    private ConcurrentHashMap<Long, Promise<RpcProto.RpcResponse>> waitingResponses;
    private MinClientHandler clientHandler;
    private EventLoopGroup group; // 创建一个NioEventLoopGroup对象，它负责处理I/O操作的多线程事件循环
    private ClientService clientService;
    private TxService txService;
    private AdminService adminService;

    public MinClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.requestId = new AtomicLong(0);
        this.waitingResponses = new ConcurrentHashMap<>();
        this.clientHandler = new MinClientHandler(waitingResponses);
        this.group = new NioEventLoopGroup(1);
        // 连接服务端
        connect();

        this.clientService = new ClientService(channel, requestId, waitingResponses, group);
        this.txService = new TxService(channel, requestId, waitingResponses, group);
        this.adminService = new AdminService(channel, requestId, waitingResponses, group);
    }

    private void connect() {
        try {
            Bootstrap bootstrap = new Bootstrap(); // 创建一个Bootstrap对象，它是Netty应用程序的入口点
            bootstrap.group(group) // 设置EventLoopGroup，用于处理I/O操作
                    .channel(NioSocketChannel.class) // 指定用于通信的Channel类型
                    .handler(new ChannelInitializer<SocketChannel>() { // 添加一个ChannelInitializer，用于初始化新连接的Channel
                        @Override // 覆盖ChannelInitializer中的初始化方法
                        protected void initChannel(SocketChannel ch) throws Exception { // 初始化Channel
                            ch.pipeline().addLast(new RpcFrameDecoder());
                            ch.pipeline().addLast(new RpcRequestEncoder());
                            ch.pipeline().addLast(new RpcResponseDecoder());
                            ch.pipeline().addLast(clientHandler);
                        }
                    });

            channel = bootstrap.connect(host, port).sync().channel(); // 使用Bootstrap连接服务器，同步连接并获取到Channel
        } catch (Exception e) {
            group.shutdownGracefully();
            throw new RuntimeException(e);
        }
    }

    public void close() {
        channel.close();
        group.shutdownGracefully();
    }


    public ClientTable getTable(String tableName) {
        return new ClientTableImpl(tableName, clientService);
    }

    public Transaction beginTransaction() throws TransactionNotExistException {
        ClientProto.BeginTransactionRequest.Builder builder = ClientProto.BeginTransactionRequest.newBuilder();
        ClientProto.BeginTransactionResponse beginTransactionResponse = clientService.beginTransaction(builder.build());
        if (beginTransactionResponse.getStatusCode() != StatusCode.SUCCESS.getCode()) {
            throw new TransactionNotExistException();
        }
        return new ClientTransaction(beginTransactionResponse.getTxid(), clientService, txService);
    }

    public boolean createTable(String tableName) {
        AdminProto.CreateTableRequest.Builder builder = AdminProto.CreateTableRequest.newBuilder();
        AdminProto.CreateTableRequest createTableRequest = builder.setTableName(ByteString.copyFromUtf8(tableName)).build();
        AdminProto.CreateTableResponse createTableResponse = adminService.createTable(createTableRequest);
        return createTableResponse.getStatusCode() == StatusCode.SUCCESS.getCode();
    }

    public boolean dropTable(String tableName) {
        AdminProto.DropTableRequest.Builder builder = AdminProto.DropTableRequest.newBuilder();
        AdminProto.DropTableRequest dropTableRequest = builder.setTableName(ByteString.copyFromUtf8(tableName)).build();
        AdminProto.DropTableResponse dropTableResponse = adminService.dropTable(dropTableRequest);
        return dropTableResponse.getStatusCode() == StatusCode.SUCCESS.getCode();
    }
    public boolean truncateTable(String tableName) {
        AdminProto.TruncateTableRequest.Builder builder = AdminProto.TruncateTableRequest.newBuilder();
        AdminProto.TruncateTableRequest truncateTableRequest = builder.setTableName(ByteString.copyFromUtf8(tableName)).build();
        AdminProto.TruncateTableResponse truncateTableResponse = adminService.truncateTable(truncateTableRequest);
        return truncateTableResponse.getStatusCode() == StatusCode.SUCCESS.getCode();
    }

    public List<TableInfo> listTables() {
        AdminProto.ListTablesRequest.Builder builder = AdminProto.ListTablesRequest.newBuilder();
        AdminProto.ListTablesRequest listTablesRequest = builder.build();
        AdminProto.ListTablesResponse listTablesResponse = adminService.listTables(listTablesRequest);
        return ProtobufUtil.toTableInfos(listTablesResponse);
    }

}
