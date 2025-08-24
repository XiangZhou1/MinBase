package org.minbase.server.table;

import org.minbase.common.exception.TransactionException;
import org.minbase.common.exception.TransactionNotExistException;
import org.minbase.common.table.ClientTable;
import org.minbase.common.table.op.*;
import org.minbase.common.table.op.ColumnValues;
import org.minbase.common.conf.Configuration;
import org.minbase.server.constant.Constants;
import org.minbase.common.exception.TableNotExistException;
import org.minbase.server.kv.store.Store;
import org.minbase.server.kv.store.StoreManager;
import org.minbase.server.table.wal.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    private final ReentrantReadWriteLock tableUpdateLock = new ReentrantReadWriteLock();

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
        tableUpdateLock.writeLock().lock();
        try {
            ConcurrentHashMap<String, Store> stores = storeManager.getStores();
            for (String tableName : stores.keySet()) {
                TableImpl table = new TableImpl(tableName, this);
                tableMap.put(tableName, table);
            }
        } finally {
            tableUpdateLock.writeLock().unlock();
        }
    }

    public Table getTable(String tableName) {
        tableUpdateLock.readLock().lock();
        try {
            return tableMap.get(tableName);
        } finally {
            tableUpdateLock.readLock().unlock();
        }
    }

    public boolean createTable(String tableName) throws IOException {
        LOG.info("Try reate table:{}", tableName);
        try {
            if (tableMap.containsKey(tableName)) {
                return true;
            }
            storeManager.createStor(tableName);
            TableImpl table = new TableImpl(tableName, this);
            tableMap.put(tableName, table);
            return true;
        } catch (Exception e) {
            LOG.error("Create table " + tableName + " fail", e);
            return false;
        }
    }

    public ColumnValues get(String table, Get get) throws IOException {
        TableImpl table1 = tableMap.get(table);
        if (table1 == null) {
            throw new TableNotExistException(table + "noe exist");
        }
        return table1.get(get);
    }

    public void put(String table, Put put) throws IOException {
        TableImpl table1 = tableMap.get(table);
        if (table1 == null) {
            throw new TableNotExistException(table + "noe exist");
        }
        table1.put(put);
    }

    public boolean containTable(String table) {
        return storeManager.containStore(table);
    }

    public boolean checkAndPut(String table, CheckAndPut checkAndPut) throws IOException {
        TableImpl table1 = tableMap.get(table);
        if (table1 == null) {
            throw new TableNotExistException(table + "noe exist");
        }
        return table1.checkAndPut(checkAndPut.getKey(),
                checkAndPut.getColumn(), checkAndPut.getValue(), checkAndPut.getPut());
    }

    public void delete(String table, Delete delete) throws IOException {
        TableImpl table1 = tableMap.get(table);
        if (table1 == null) {
            throw new TableNotExistException(table + "noe exist");
        }
        table1.delete(delete);
    }

    public Transaction newTransaction() {
        Transaction transaction = transactionManager.newTransaction();
        return transaction;
    }

    public void txPut(long txid, String table, Put put) throws IOException {
        Transaction activeTransaction = transactionManager.getActiveTransaction(txid);
        if (activeTransaction == null) {
            throw new TransactionNotExistException();
        }
        Table table1 = activeTransaction.getTable(table);
        table1.put(put);
    }

    public ColumnValues txGet(long txid, String table, Get get) throws IOException {
        Transaction activeTransaction = transactionManager.getActiveTransaction(txid);
        if (activeTransaction == null) {
            throw new TransactionNotExistException();
        }
        Table table1 = activeTransaction.getTable(table);
        return table1.get(get);
    }
    public void txDelete(long txid, String table, Delete delete) throws IOException {
        Transaction activeTransaction = transactionManager.getActiveTransaction(txid);
        if (activeTransaction == null) {
            throw new TransactionNotExistException();
        }
        Table table1 = activeTransaction.getTable(table);
        table1.delete(delete);
    }

    public boolean txCheckAndPut(long txid, String table, CheckAndPut checkAndPut) throws IOException {
        Transaction activeTransaction = transactionManager.getActiveTransaction(txid);
        if (activeTransaction == null) {
            throw new TransactionNotExistException();
        }
        Table table1 = activeTransaction.getTable(table);
        return table1.checkAndPut(checkAndPut.getKey(), checkAndPut.getColumn(), checkAndPut.getValue(), checkAndPut.getPut());
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
                long clearOldLogCheckINterval = configuration.getLong(Constants.CLEAR_OLD_LOG_CHECK_INTERVAL_KEY,
                        Constants.CLEAR_OLD_LOG_CHECK_INTERVAL_DEFAULT);
                synchronized (clearOldLogLock) {
                    clearOldLogLock.wait(clearOldLogCheckINterval);
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

    public String[] listTableNames() {
        tableUpdateLock.readLock().lock();
        try {
            return tableMap.keySet().toArray(new String[0]);
        } finally {
            tableUpdateLock.readLock().unlock();
        }
    }

    public boolean dropTable(String tableName) {
        tableUpdateLock.readLock().lock();
        try {
            tableMap.remove(tableName);
            return true;
        } finally {
            tableUpdateLock.readLock().unlock();
        }
    }

    public void rollBackTransaction(long txId) {
        Transaction activeTransaction = transactionManager.getActiveTransaction(txId);
        if (activeTransaction != null) {
            activeTransaction.rollback();
        }
    }

    public void rollBackTransactions(List<Long> sessionTransactions) {
        for (Long sessionTransaction : sessionTransactions) {
            Transaction activeTransaction = transactionManager.getActiveTransaction(sessionTransaction);
            if (activeTransaction != null) {
                activeTransaction.rollback();
            }
        }
    }

    public void commitTransaction(long txid) throws TransactionNotExistException, TransactionException {
        Transaction activeTransaction = transactionManager.getActiveTransaction(txid);
        if (activeTransaction != null) {
            activeTransaction.commit();
        } else {
            throw new TransactionNotExistException();
        }
    }


}
