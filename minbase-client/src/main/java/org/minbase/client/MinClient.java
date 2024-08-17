package org.minbase.client;


import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Promise;
import org.minbase.client.admin.Admin;
import org.minbase.client.admin.ClientAdmin;
import org.minbase.client.handler.MinClientHandler;
import org.minbase.client.service.AdminService;
import org.minbase.client.service.ClientService;
import org.minbase.client.service.TxService;
import org.minbase.client.table.ClientTable;
import org.minbase.client.transaction.ClientTransaction;
import org.minbase.common.rpc.codec.RpcFrameDecoder;
import org.minbase.common.rpc.codec.RpcRequestEncoder;
import org.minbase.common.rpc.codec.RpcResponseDecoder;
import org.minbase.common.rpc.proto.generated.AdminProto;
import org.minbase.common.rpc.proto.generated.RpcProto;
import org.minbase.common.table.Table;
import org.minbase.common.transaction.Transaction;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MinClient {
    private final String host;
    private final int port;
    private final Admin admin;
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
        this.admin = new ClientAdmin(this.adminService);
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


    public Table getTable(String tableName) {
        return new ClientTable(tableName, clientService);
    }

    public Transaction beginTransaction() {
        ClientProto.BeginTransactionRequest.Builder builder = ClientProto.BeginTransactionRequest.newBuilder();
        ClientProto.BeginTransactionResponse beginTransactionResponse = clientService.beginTransaction(builder.build());
        if (!beginTransactionResponse.getSuccess()) {
            return null;
        }
        return new ClientTransaction(beginTransactionResponse.getTxid(), clientService, txService);
    }

    public boolean createTable(String tableName) {
        AdminProto.CreateTableRequest.Builder builder = AdminProto.CreateTableRequest.newBuilder();
        AdminProto.CreateTableRequest createTableRequest = builder.setTableName(tableName).build();
        AdminProto.CreateTableResponse createTableResponse = adminService.createTable(createTableRequest);
        return createTableResponse.getSuccess();
    }

    public Admin getAdmin() {
        return admin;
    }

    public List<String> getTableInfo(String tableName) {
        return this.admin.getTableInfo(tableName);
    }
}
