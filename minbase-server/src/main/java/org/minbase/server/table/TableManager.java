package org.minbase.server.table;

import org.minbase.common.table.Table;
import org.minbase.common.table.op.*;
import org.minbase.common.table.op.ColumnValues;
import org.minbase.server.conf.Configuration;
import org.minbase.server.constant.Constants;
import org.minbase.server.kv.store.Store;
import org.minbase.server.kv.store.StoreManager;
import org.minbase.server.table.wal.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TableManager {
    private static final Logger LOG = LoggerFactory.getLogger(TableManager.class);
    private final ExecutorService clearOldLogExecutor;
    private Configuration configuration;

    private StoreManager storeManager;
    private File tableManagerDir;
    private TransactionManager transactionManager;
    private Map<String, TableImpl> tableMap = new HashMap<>();
    private Wal wal;
    private final Object clearOldLogLock = new Object();

    public TableManager(Configuration configuration) throws IOException {
        this.configuration = configuration;
        this.tableManagerDir = new File(configuration.get(Constants.STORE_DIR_KEY, Constants.STORE_DIR_DEFAULT));
        this.storeManager = new StoreManager(new File(tableManagerDir, "store"), configuration, this);
        this.transactionManager = new TransactionManager(this);
        this.wal = new Wal(new File(tableManagerDir, "wal"), this);

        initTable();
        wal.recovery();
        this.clearOldLogExecutor = Executors.newSingleThreadExecutor();
        this.clearOldLogExecutor.submit(new Runnable() {
            @Override
            public void run() {
                clearOldLogTask();
            }
        });

    }

    private void initTable() {
        ConcurrentHashMap<String, Store> stores = storeManager.getStores();
        for (String tableName : stores.keySet()) {
            TableImpl table = new TableImpl(tableName, this);
            tableMap.put(tableName, table);
        }
    }

    public Table getTable(String tableName) {
        return tableMap.get(tableName);
    }

    private String[] listTableName() {
        return storeManager.listStoreNames();
    }

    public boolean createTable(String tableName) throws IOException {
        try {
            if (tableMap.containsKey(tableName)) {
                return true;
            }
            storeManager.createStor(tableName);
            TableImpl table = new TableImpl(tableName, this);
            tableMap.put(tableName, table);
        } catch (Exception e) {
            LOG.error("Create table " + tableName + " fail", e);
            return false;
        }
        return true;
    }

    public ColumnValues get(String table, Get get) {
        TableImpl table1 = tableMap.get(table);
        return table1.get(get);
    }

    public void put(String table, Put put) {
        TableImpl table1 = tableMap.get(table);
        table1.put(put);
    }

    public boolean containTable(String table) {
        return storeManager.containStore(table);
    }

    public void checkAndPut(String table, CheckAndPut checkAndPut) {
        TableImpl table1 = tableMap.get(table);
        table1.checkAndPut(checkAndPut.getKey(), checkAndPut.getColumn(), checkAndPut.getValue(), checkAndPut.getPut());
    }

    public void delete(String table, Delete delete) {
        TableImpl table1 = tableMap.get(table);
        table1.delete(delete);
    }

    public Transaction newTransaction() {
        Transaction transaction = transactionManager.newTransaction();
        return transaction;
    }

    public void txPut(long txid, String table, Put put) {
        Transaction activeTransaction = transactionManager.getActiveTransaction(txid);
        Table table1 = activeTransaction.getTable(table);
        table1.put(put);
    }

    public ColumnValues txGet(long txid, String table, Get get) {
        Transaction activeTransaction = transactionManager.getActiveTransaction(txid);
        Table table1 = activeTransaction.getTable(table);
        return table1.get(get);
    }

    public void rollBackTransaction(long txId) {

    }

    public StoreManager getStoreManager() {
        return storeManager;
    }

    public TransactionManager getTransactionManager() {
        return transactionManager;
    }

    public Map<String, TableImpl> getTableMap() {
        return tableMap;
    }

    public Wal getWal() {
        return wal;
    }

    public File getTableManagerDir() {
        return tableManagerDir;
    }

    public void applyLog(LogEntry logEntry) {
        storeManager.applyLog(logEntry.getWriteBatch());
        transactionManager.setSequenceId(logEntry.getLastSequenceId());
    }


    private void clearOldLogTask() {
        while (true) {
            try {
                synchronized (clearOldLogLock) {
                    clearOldLogLock.wait(10000);
                }
                if (wal.shouldForeFlush()) {
                    foreFlush();
                }
                long minSyncedSequenceId = storeManager.getMinFlushedSequenceId();
                wal.clearOldWal(minSyncedSequenceId);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void requestClearOldLog() {
        synchronized (clearOldLogLock) {
            clearOldLogLock.notify();
        }
    }

    public void foreFlush(String tableName) {
        storeManager.foreFlush(tableName);
    }

    public void foreFlush() {
        storeManager.foreFlush();
    }

    public void close() {
        foreFlush();
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public long getTransactionMinReadPoint() {
        return transactionManager.getTransactionMinReadPoint();
    }

    public long getReadPoint() {
        return storeManager.getReadPoint();
    }
}
