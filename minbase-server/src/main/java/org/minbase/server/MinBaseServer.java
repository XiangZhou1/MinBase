package org.minbase.server;

import org.minbase.server.rpc.RpcServer;

import org.minbase.common.conf.Configuration;
import org.minbase.server.constant.Constants;
import org.minbase.server.table.TableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class MinBaseServer {
    private static final Logger LOG = LoggerFactory.getLogger(MinBaseServer.class);
    private static MinBaseServer SERVER_INSTANCE = null;
    private Configuration configuration;
    private RpcServer rpcServer;

    private TableManager tableManager;
    private final Object stopLock = new Object();
    private boolean stop = false;
    private int port = 4444;
    private String ip = "127.0.0.1";
    public MinBaseServer(Configuration configuration) throws IOException {
        this.configuration = configuration;
        this.port = configuration.getInt(Constants.SERVER_PORT_KEY, Constants.SERVER_PORT_DEFAULT);
        this.ip = configuration.get(Constants.SERVER_IP_KEY, Constants.SERVER_IP_DEFAULT);
        this.tableManager = new TableManager(configuration);
        this.rpcServer = new RpcServer(tableManager, ip, port);
    }


    public void start() throws InterruptedException {
        this.rpcServer.start();
    }

    public void stop() throws InterruptedException {
        stop = true;
        synchronized (stopLock) {
            stopLock.notify();
        }
        LOG.info("Stop MinBaseServer");
    }

    public static void stopServer() throws InterruptedException {
        if (SERVER_INSTANCE != null) {
            SERVER_INSTANCE.stop();
        }
    }

    public static void startServer(String[] args) throws Exception {
        if (SERVER_INSTANCE != null) {
            return;
        }
        Configuration configuration;
        if (args == null || args.length == 0) {
            configuration = new Configuration();
        } else {
            configuration = new Configuration(new File(args[0]));
        }
        MinBaseServer minBaseServer = new MinBaseServer(configuration);
        minBaseServer.start();
        LOG.info("Start MinBaseServer");
        minBaseServer.waitForShutdown();
    }

    public void waitForShutdown() {
        while (!stop) {
            synchronized (stopLock) {
                try {
                    stopLock.wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        startServer(args);
    }
}
