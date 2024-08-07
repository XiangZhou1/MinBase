package org.minbase.rpc;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.minbase.common.operation.ColumnValues;
import org.minbase.common.operation.Get;
import org.minbase.common.rpc.Constant;
import org.minbase.common.rpc.codec.RpcFrameDecoder;
import org.minbase.common.rpc.codec.RpcRequestDecoder;
import org.minbase.common.rpc.codec.RpcResponseEncoder;
import org.minbase.common.rpc.proto.generated.ClientProto;
import org.minbase.common.rpc.proto.generated.ClientServiceGrpc;
import org.minbase.common.table.Table;
import org.minbase.common.utils.ProtobufUtil;
import org.minbase.server.MinBaseServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RpcServer implements ClientServiceGrpc.ClientServiceBlockingClient {
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
                            channel.pipeline().addLast(new RpcHandler(RpcServer.this));
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


    @Override
    public ClientProto.GetResponse get(ClientProto.GetRequest request) {
        Get get = ProtobufUtil.toGet(request);
        final Table table = server.getTable(request.getTable());
        if (table == null) {
            throw new RuntimeException("Table not exist, table=" + request.getTable());
        }
        final ColumnValues columnValues = table.get(get);
        return ProtobufUtil.toGetResponse(request.getKey(), columnValues);
    }

    @Override
    public ClientProto.PutResponse put(ClientProto.PutRequest request) {
        return null;
    }

    @Override
    public ClientProto.CheckAndPutResponse checkAndPut(ClientProto.CheckAndPutRequest request) {
        return null;
    }

    @Override
    public ClientProto.DeleteResponse delete(ClientProto.DeleteRequest request) {
        return null;
    }
}
