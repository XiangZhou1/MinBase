package org.minbase.server;

import org.minbase.common.table.Table;
import org.minbase.server.kv.compaction.CompactionPolicy;
import org.minbase.server.kv.storage.StoreFile;
import org.minbase.server.kv.store.StoreManager;
import org.minbase.server.rpc.RpcServer;

import org.minbase.server.conf.Configuration;
import org.minbase.server.constant.Constants;
import org.minbase.server.kv.storage.StoreFileManager;
import org.minbase.server.kv.store.Store;
import org.minbase.server.table.TableImpl;
import org.minbase.server.table.transaction.Transaction;
import org.minbase.server.table.transaction.TransactionManager;
import org.minbase.server.kv.wal.Wal;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class MinBaseServer {
    public static final String Data_Dir = Configuration.get(Constants.KEY_DATA_DIR);

    private Configuration configuration;

    private ConcurrentHashMap<String, TableImpl> tables;
    private RpcServer rpcServer;

    private StoreManager storeManager;

    public MinBaseServer(Configuration configuration) throws IOException {
        this.configuration = configuration;
        this.rpcServer = new RpcServer(this);
    }

    private void init() {
        // wal 日
    }

    private File[] listTableDirs() {
        final File dir = new File(Data_Dir);
        return dir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.isDirectory();
            }
        });
    }

    public void startRpcServer() throws InterruptedException {
        this.rpcServer.start();
    }

    public Table getTable(String tableName) {
        return tables.get(tableName);
    }

    public Table createTable(String tableName) throws IOException {
//        File tableDir = new File(Data_Dir, tableName);
//        if (!tableDir.exists()) {
//            if (!tableDir.mkdirs()) {
//                throw new IOException("create table fail");
//            }
//        }
//        final TableImpl table = new TableImpl(tableName, store);
//        tables.put(tableName, table);
//        return table;
        return null;
    }

    public Transaction newTransaction() {
        return TransactionManager.newTransaction(tables);
    }



    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration configuration = new Configuration();
        MinBaseServer minBaseServer = new MinBaseServer(configuration);
        minBaseServer.init();
        minBaseServer.startRpcServer();
    }


}
