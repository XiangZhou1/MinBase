package org.minbase.server.rpc;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.minbase.common.rpc.Constant;
import org.minbase.common.rpc.codec.RpcFrameDecoder;
import org.minbase.common.rpc.codec.RpcRequestDecoder;
import org.minbase.common.rpc.codec.RpcResponseEncoder;
import org.minbase.server.MinBaseServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RpcServer {
    private static final Logger logger = LoggerFactory.getLogger(RpcServer.class);

    private int port;
    private ServerBootstrap serverBootstrap;
    private MinBaseServer server;

    public RpcServer(MinBaseServer server, int port) {
        this.port = port;
        this.server = server;
    }

    public RpcServer(MinBaseServer server) {
        this(server, Constant.DEFAULT_SERVER_PORT);
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

            logger.info("Start rpcServer, bind port:" + port);
            ChannelFuture channelFuture = serverBootstrap.bind(port).sync();
            channelFuture.channel().closeFuture().sync();
        } catch (Exception e) {
            logger.info("Start rpcServer fail", e);
        } finally {
            eventLoopGroup.shutdownGracefully();
        }
    }



}
