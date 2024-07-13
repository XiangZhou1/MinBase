package org.minbase.client;


import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Promise;
import org.minbase.client.handler.ClientHandler;
import org.minbase.common.rpc.RpcRequest;
import org.minbase.common.rpc.RpcResponse;
import org.minbase.common.rpc.codec.*;
import org.minbase.common.rpc.service.ClientService;
import org.minbase.common.rpc.service.Methods;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class Client implements ClientService {
    private final String host;
    private final int port;
    private Channel channel;
    private AtomicLong requestId;
    private ConcurrentHashMap<Long, Promise<RpcResponse>> waitingResponses;
    private ClientHandler clientHandler;
    private EventLoopGroup group = new NioEventLoopGroup(); // 创建一个NioEventLoopGroup对象，它负责处理I/O操作的多线程事件循环
    public Client(String host, int port) {
        this.host = host;
        this.port = port;
        this.requestId = new AtomicLong(0);
        this.waitingResponses = new ConcurrentHashMap<>();
        clientHandler = new ClientHandler(waitingResponses);

        // 连接服务端
        connect();
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


    @Override
    public String get(String key) {
        long id = requestId.incrementAndGet();
        RpcRequest rpcRequest = new RpcRequest(id, Methods.GET.getName(), new Object[]{key});
        RpcResponse rpcResponse = call(rpcRequest);
        return (String) rpcResponse.getValue();
    }

    @Override
    public void put(String key, String value) {
        long id = requestId.incrementAndGet();
        final RpcRequest rpcRequest = new RpcRequest(id, Methods.PUT.getName(), new Object[]{key, value});
        call(rpcRequest);
    }

    @Override
    public boolean checkAndPut(String checkKey, String checkValue, String key, String value) {
        return false;
    }

    @Override
    public void delete(String key) {

    }

    private RpcResponse call(RpcRequest rpcRequest) {
        Promise<RpcResponse> responsePromise = new DefaultPromise<>(group.next());
        waitingResponses.put(rpcRequest.getId(), responsePromise);
        channel.writeAndFlush(rpcRequest);
        try {
            final RpcResponse rpcResponse = responsePromise.get();
            if (rpcResponse.getCode() == -1) {
                throw new RuntimeException("rpc fail");
            }
            return rpcResponse;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        channel.close();
        group.shutdownGracefully();
    }
}
