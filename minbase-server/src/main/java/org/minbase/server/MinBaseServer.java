package org.minbase.server;

import org.minbase.common.table.Table;
import org.minbase.server.rpc.RpcServer;
import org.minbase.server.kv.compaction.CompactThread;
import org.minbase.server.kv.compaction.Compaction;
import org.minbase.server.kv.compaction.CompactionStrategy;
import org.minbase.server.kv.compaction.level.LevelCompaction;

import org.minbase.server.kv.compaction.tiered.TieredCompaction;
import org.minbase.server.conf.Configuration;
import org.minbase.server.constant.Constants;
import org.minbase.server.kv.storage.storefilemanager.AbstractStoreFileManager;
import org.minbase.server.kv.store.Store;
import org.minbase.server.table.TableImpl;
import org.minbase.server.table.transaction.Transaction;
import org.minbase.server.table.transaction.TransactionManager;
import org.minbase.server.kv.wal.Wal;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class MinBaseServer {
    public static final String Data_Dir = Configuration.get(Constants.KEY_DATA_DIR);

    private Configuration configuration;

    private ConcurrentHashMap<String, TableImpl> tables;
    private RpcServer rpcServer;

    private AtomicLong sequenceId;

    // 文件刷写线程
    private ThreadPoolExecutor flushThreadPool;

    // 文件压缩线程
    private Compaction compaction;
    private CompactThread compactThread;

    private Wal wal;

    public MinBaseServer(Configuration configuration) throws IOException {
        this.configuration = configuration;
        this.rpcServer = new RpcServer(this);
        // 刷写线程
        int flushThreadMaxPoolSize = configuration.getInt(Constants.FLUSH_THREAD_MAX_POOL_SIZE_KEY, Constants.FLUSH_THREAD_MAX_POOL_SIZE_DEFAULT);
        flushThreadPool = new ThreadPoolExecutor(1, flushThreadMaxPoolSize, 1L, TimeUnit.MINUTES, new ArrayBlockingQueue<>(100));
    }

    private void init() throws IOException {
        // wal 日志
        wal = new Wal();
        String compactionStrategy = Configuration.get(Constants.KEY_COMPACTION_STRATEGY);
        if (CompactionStrategy.LEVEL_COMPACTION.toString().equals(compactionStrategy)) {
            this.compaction = new LevelCompaction();
        } else if (CompactionStrategy.TIERED_COMPACTION.toString().equals(compactionStrategy)) {
            this.compaction = new TieredCompaction();
        }

        this.compactThread = new CompactThread(this.compaction, tables);

        File[] tableDirs = listTableDirs();
        for (File tableDir : tableDirs) {
            String tableName = tableDir.getName();
            Store store = new Store(tableName, tableDir, configuration, flushThreadPool, compaction, compactThread);
            tables.put(tableDir.getName(), new TableImpl(tableDir.getName(), store));
        }
        wal.recovery(tables);

        // 压缩线程
        this.compactThread.start();
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
        File tableDir = new File(Data_Dir, tableName);
        if (!tableDir.exists()) {
            if (!tableDir.mkdirs()) {
                throw new IOException("create table fail");
            }
        }
        Store store = new Store(tableName, tableDir, configuration, flushThreadPool, compaction, compactThread);
        final TableImpl table = new TableImpl(tableName, store);
        tables.put(tableName, table);
        return table;
    }

    public Transaction newTransaction() {
        return TransactionManager.newTransaction(tables);
    }

    public void compact(String tableName) throws Exception {
        final TableImpl table = tables.get(tableName);
        if (table == null) {
            return;
        }

        final AbstractStoreFileManager storageManager = table.getMinStore().getStorageManager();
        if (compaction.needCompact(storageManager)) {
            this.compaction.compact(storageManager);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration configuration = new Configuration();
        MinBaseServer minBaseServer = new MinBaseServer(configuration);
        minBaseServer.init();
        minBaseServer.startRpcServer();
    }


}
