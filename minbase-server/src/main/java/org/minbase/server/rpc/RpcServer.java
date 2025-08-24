package org.minbase.server.rpc;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.minbase.common.rpc.codec.RpcFrameDecoder;
import org.minbase.common.rpc.codec.RpcRequestDecoder;
import org.minbase.common.rpc.codec.RpcResponseEncoder;
import org.minbase.server.table.TableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RpcServer {
    private static final Logger LOG = LoggerFactory.getLogger(RpcServer.class);
    private final int port;
    private final String ip;
    private ServerBootstrap serverBootstrap;
    private final TableManager server;


    public RpcServer(TableManager server, String ip, int port) {
        this.ip = ip;
        this.port = port;
        this.server = server;
    }

    public void start() throws InterruptedException {
        final NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        try {
            this.serverBootstrap = new ServerBootstrap()
                    .group(eventLoopGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) throws Exception {
                            channel.pipeline().addLast(new RpcFrameDecoder());
                            channel.pipeline().addLast(new RpcResponseEncoder());
                            channel.pipeline().addLast(new RpcRequestDecoder());
                            channel.pipeline().addLast(new RpcHandler(server));
                        }
                    });

            LOG.info("Start rpcServer, bind address: {}:{}", ip, port);
            ChannelFuture channelFuture = serverBootstrap.bind(ip, port).sync();
            channelFuture.channel().closeFuture().sync();
        } catch (Exception e) {
            LOG.info("Start rpcServer fail", e);
        } finally {
            eventLoopGroup.shutdownGracefully();
        }
    }
}
