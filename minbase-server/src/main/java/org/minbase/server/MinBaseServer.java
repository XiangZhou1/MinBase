package org.minbase.server;

import org.minbase.common.table.op.*;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.kv.*;
import org.minbase.server.kv.store.Scanner;
import org.minbase.server.kv.store.StoreManager;
import org.minbase.server.rpc.RpcServer;

import org.minbase.server.conf.Configuration;
import org.minbase.server.constant.Constants;
import org.minbase.server.table.TableKey;
import org.minbase.server.table.WriteBatchUtil;
import org.minbase.server.table.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class MinBaseServer {
    private static final Logger LOG = LoggerFactory.getLogger(MinBaseServer.class);
    public static final String Data_Dir = Configuration.get(Constants.KEY_DATA_DIR);
    private Configuration configuration;
    private RpcServer rpcServer;

    private StoreManager storeManager;
    private Object stopLock = new Object();
    private boolean stop = false;
    File storeManagerDir;

    public MinBaseServer(Configuration configuration) throws IOException {
        this.configuration = configuration;
        this.rpcServer = new RpcServer(this);
    }

    private String[] listTableName() {
        return storeManager.listStoreNames();
    }

    public void start() throws InterruptedException {
        this.rpcServer.start();
    }

    public boolean createTable(String tableName) throws IOException {
        try {
            File tableDir = new File(storeManagerDir, tableName);
            if (!tableDir.exists()) {
                if (!tableDir.mkdirs()) {
                    throw new IOException("create table fail");
                }
            }
            storeManager.createStor(tableName);
        } catch (Exception e) {
            LOG.error("Create table " + tableName + " fail", e);
            return false;
        }
        return true;
    }

    public Transaction newTransaction() {
        //return TransactionManager.newTransaction(tables);
        return null;
    }



    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration configuration = new Configuration();
        MinBaseServer minBaseServer = new MinBaseServer(configuration);
        minBaseServer.start();
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

    public ColumnValues get(String table, Get get) {
        List<byte[]> columns = get.getColumns();
        byte[] key = get.getKey();
        columns.sort(ByteUtil.BYTE_ORDER_COMPARATOR);

        TableKey tableKeyFirst = new TableKey(key, columns.get(0));
        TableKey tableKeyLast = new TableKey(key, columns.get(columns.size() - 1));

        Key startKey = new Key(tableKeyLast.encode(), Long.MAX_VALUE);
        Key endKey = new Key(tableKeyFirst.encode(), 0);
        Scanner scan = storeManager.scan(table, startKey, endKey);

        ColumnValues columnValues = new ColumnValues();
        while (scan.hasNext()) {
            KeyValue keyValue = scan.next();
            if (keyValue == null) {
                continue;
            }
            TableKey tableKey = new TableKey();
            tableKey.decode(keyValue.getKey().getInternalKey());
            byte[] column = tableKey.getColumn();
            if (columns.contains(column)) {
                columnValues.set(column, keyValue.getValue().getValue());
            }
        }
        return columnValues;
    }

    public void put(String table, Put put) {
        WriteBatch writeBatch = WriteBatchUtil.fromPut(table, put);
        storeManager.put(writeBatch);
    }

    public boolean containTable(String table) {
        return storeManager.containStore(table);
    }

    public void checkAndPut(String table, CheckAndPut checkAndPut) {

    }

    public void delete(String table, Delete delete) {
        WriteBatch writeBatch = WriteBatchUtil.fromDelete(table, delete);
        storeManager.put(writeBatch);
    }
}
