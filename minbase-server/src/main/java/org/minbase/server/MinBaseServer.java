package org.minbase.server;


import org.minbase.common.utils.FileUtil;
import org.minbase.server.rpc.RpcServer;
import org.minbase.server.compaction.CompactThread;
import org.minbase.server.compaction.Compaction;
import org.minbase.server.compaction.CompactionStrategy;
import org.minbase.server.compaction.level.LevelCompaction;
import org.minbase.server.compaction.tiered.TieredCompaction;
import org.minbase.server.conf.Config;
import org.minbase.server.constant.Constants;
import org.minbase.server.minstore.MinStore;
import org.minbase.server.table.Table;
import org.minbase.server.table.TableMeta;
import org.minbase.server.transaction.Transaction;
import org.minbase.server.transaction.TransactionManager;
import org.minbase.server.wal.Wal;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class MinBaseServer {
    public static final String Data_Dir = Config.DATA_DIR;

    private ConcurrentHashMap<String, Table> tables;
    private RpcServer rpcServer;
    // 文件刷写线程
    private Executor flushThread;

    // 文件压缩线程
    private Compaction compaction;
    private CompactThread compactThread;

    private Wal wal;

    public MinBaseServer() throws IOException {
        this.rpcServer = new RpcServer(this);
        // 刷写线程
        flushThread = Executors.newSingleThreadExecutor();
    }

    private void init() throws IOException {
        // wal 日志
        wal = new Wal();
        String compactionStrategy = Config.get(Constants.KEY_COMPACTION_STRATEGY);
        if (CompactionStrategy.LEVEL_COMPACTION.toString().equals(compactionStrategy)) {
            this.compaction = new LevelCompaction();
        } else if (CompactionStrategy.TIERED_COMPACTION.toString().equals(compactionStrategy)) {
            this.compaction = new TieredCompaction();
        }

        this.compactThread = new CompactThread(this.compaction, tables);

        File[] tableDirs = listTableDirs();
        for (File tableDir : tableDirs) {
            String tableName = tableDir.getName();
            TableMeta tableMeta = loadTableMeta(tableName);
            MinStore minStore = new MinStore(tableName, tableDir, flushThread, compaction, compactThread);
            tables.put(tableDir.getName(), new Table(tableMeta, minStore));
        }
        wal.recovery(this);
        // 压缩线程
        this.compactThread.start();
    }

    private TableMeta loadTableMeta(String tableName) throws IOException {
        File metaFile = new File(Config.DATA_DIR, tableName + File.separator + ".tableMeta");
        byte[] buf = FileUtil.read(metaFile);
        TableMeta tableMeta = new TableMeta();
        tableMeta.decode(buf);
        return tableMeta;
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

    public Table createTable(String tableName, List<String> columns) throws IOException {
        File tableDir = new File(Data_Dir, tableName);
        if (!tableDir.exists()) {
            if (!tableDir.mkdirs()) {
                throw new IOException("create table fail");
            }
        }
        TableMeta tableMeta = new TableMeta(tableName, columns);
        MinStore minStore = new MinStore(tableName, tableDir, flushThread, compaction, compactThread);
        Table table = new Table(tableMeta, minStore);
        table.updateTableMeta();
        tables.put(tableName, table);
        return table;
    }

    public Transaction newTransaction() {
        return TransactionManager.newTransaction(tables);
    }

//    public void compact(String tableName) throws Exception {
//        final AutoTxTable table = tables.get(tableName);
//        if (table == null) {
//            return;
//        }
//
//        final StoreManager storageManager = table.getMinStore().getStorageManager();
//        if (compaction.needCompact(storageManager)) {
//            this.compaction.compact(storageManager);
//        }
//    }


    public boolean addColumns(String tableName, List<String> columns) {
        try {
            Table table = tables.get(tableName);
            if (table == null) {
                return false;
            }
            table.addColumns(columns);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public boolean dropTable(String tableName) {
        tables.remove(tableName);
        File storeFile = new File(Config.DATA_DIR, tableName);
        FileUtil.deleteFiles(storeFile);
        return true;
    }

    public boolean truncateTable(String tableName) {
        try{
            Table table = tables.get(tableName);
            List<String> columns = table.getColumns();
            if(dropTable(tableName)){
                createTable(tableName, columns);
            }
            return true;
        }catch (IOException e){
            return false;
        }
    }
}
