package org.minbase.server.kv.store;

import org.minbase.common.conf.Configuration;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.WriteBatch;
import org.minbase.server.kv.iterator.KeyValueIterator;
import org.minbase.server.kv.utils.KeyUtil;
import org.minbase.server.table.TableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StoreManager {
    private static final Logger LOG = LoggerFactory.getLogger(StoreManager.class);
    private Configuration configuration;
    private ConcurrentHashMap<String, Store> stores;
    private MultiVersionControler mvcc = new MultiVersionControler();
    private ReadWriteLock readWriteLock;
    private File storeManagerDir;
    private TableManager tableManager;
    private PriorityQueue<ScannerInfo> scanners = new PriorityQueue<>(new Comparator<ScannerInfo>() {
        @Override
        public int compare(ScannerInfo o1, ScannerInfo o2) {
            if (o1.getReadPoint() < o2.getReadPoint()) {
                return -1;
            } else if (o1.getReadPoint() > o2.getReadPoint()) {
                return 1;
            } else {
                return 0;
            }
        }
    });
    private ReentrantReadWriteLock scannerUpdateLock = new ReentrantReadWriteLock();

    public StoreManager(File storeManagerDir, Configuration configuration, TableManager tableManager) throws IOException {
        this.configuration = configuration;
        this.storeManagerDir = storeManagerDir;
        this.readWriteLock = new ReentrantReadWriteLock();
        this.stores = new ConcurrentHashMap<>();
        loadStores();
        this.tableManager = tableManager;
    }

    private void loadStores() throws IOException {
        File[] storeDirs = storeManagerDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.isDirectory();
            }
        });
        if (storeDirs == null || storeDirs.length == 0) {
            return;
        }
        for (File storeDir : storeDirs) {
            String storeName = storeDir.getName();
            Store store = new Store(storeDir.getName(), storeDir, configuration, this);
            stores.put(storeName, store);
        }
    }

    public Store createStor(String storeName) throws IOException {
        File storeDir = new File(storeManagerDir, storeName);
        Store store = new Store(storeName, storeDir, configuration, this);
        stores.put(storeName, store);
        return store;
    }

    public Store getStore(String storeName) {
        return stores.get(storeName);
    }

    public Store deleteStore(String storeName) {
        return stores.remove(storeName);
    }

    public void put(WriteBatch writeBatch) {
        List<String> storeNames = writeBatch.getStoreNames();
        for (String storeName : storeNames) {
            Store store = stores.get(storeName);
            store.put(writeBatch.getKeyValues(storeName));
        }
        mvcc.completeWrite(writeBatch.getLastSequenceId());
    }

    public KeyValue get(String store, byte[] key) {
        Scanner scan = scan(store, new Key(key, mvcc.getReadPoint()), KeyUtil.earliestVersionKey(key));
        if (scan.hasNext()) {
            KeyValue next = scan.next();
            if (next != null && next.getValue().isPut()) {
                return next;
            }
        }
        return null;
    }

    public Scanner scan(String store, Key startKey, Key endKey, long readPoint) {
        scannerUpdateLock.writeLock().lock();
        ScannerInfo scannerInfo = null;
        try {
            scannerInfo = new ScannerInfo(ScannerInfo.SCANNER_ID.incrementAndGet(), readPoint);
            scanners.add(scannerInfo);
        } finally {
            scannerUpdateLock.writeLock().unlock();
        }
        KeyValueIterator iterator = stores.get(store).iterator(startKey, endKey);
        Scanner scanner = new Scanner(iterator, readPoint);
        scanner.setStoreManager(this);
        scanner.setScannerInfo(scannerInfo);
        return scanner;
    }

    public Scanner scan(String store, Key startKey, Key endKey) {
        return scan(store, startKey, endKey, mvcc.getReadPoint());
    }

    public void writeLock() {
        this.readWriteLock.writeLock().lock();
    }

    public void writeUnLock() {
        this.readWriteLock.writeLock().unlock();
    }

    public void readLock() {
        this.readWriteLock.readLock().lock();
    }

    public void readUnLock() {
        this.readWriteLock.readLock().unlock();
    }

    public void applyLog(WriteBatch writeBatch) {
        List<String> storeNames = writeBatch.getStoreNames();
        for (String storeName : storeNames) {
            Store store = stores.get(storeName);
            store.put(writeBatch.getKeyValues(storeName));
        }
        mvcc.completeWrite(writeBatch.getLastSequenceId());
        mvcc.setWritePoint(writeBatch.getLastSequenceId());
    }

    /**
     * readPoint含义: 高于readPoint的op不可见
     * mingReadPoint含义: 用于compaction, 低于minReadPiont的op可以合并
     */
    public long getMinReadPoint() {
        scannerUpdateLock.readLock().lock();
        try {
            long minReadPointOfTransaction = tableManager.getTransactionMinReadPoint();
            ScannerInfo peek = scanners.peek();
            if (peek == null) {
                return Math.min(mvcc.getReadPoint(), minReadPointOfTransaction) ;
            } else {
                return Math.min(peek.getReadPoint(), minReadPointOfTransaction);
            }
        } finally {
            scannerUpdateLock.readLock().unlock();
        }
    }

    public void removeScanner(ScannerInfo scannerInfo) {
        scannerUpdateLock.writeLock().lock();
        try {
            this.scanners.remove(scannerInfo);
        } finally {
            scannerUpdateLock.writeLock().unlock();
        }
    }

    public long getMinFlushedSequenceId() {
        long minFlusheedSequenceId = 0;
        for (Store store : stores.values()) {
            minFlusheedSequenceId = Math.max(minFlusheedSequenceId, store.getFlushedSequenceId());
        }
        return minFlusheedSequenceId;
    }

    public void requestClearOldLog() {
        tableManager.requestClearOldLog();
    }

    public void foreFlush() {
        stores.forEach((storeName, store) -> {
            store.foreFlush();
        });
    }

    public void foreFlush(String storeName) {
        Store store = stores.get(storeName);
       store.foreFlush();
    }

    public boolean containStore(String storeName) {
        return stores.containsKey(storeName);
    }

    public String[] listStoreNames() {
        return stores.keySet().toArray(new String[0]);
    }

    public ConcurrentHashMap<String, Store> getStores() {
        return stores;
    }

    public long getReadPoint() {
        return mvcc.getReadPoint();
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}
