package org.minbase.server;


import org.minbase.server.constant.Constants;
import org.minbase.server.iterator.KeyIterator;
import org.minbase.server.iterator.SnapshotIterator;
import org.minbase.server.lsmStorage.LsmStorage;
import org.minbase.server.op.Key;
import org.minbase.server.op.WriteBatch;
import org.minbase.server.transaction.*;

import java.io.IOException;

public class MinBase {
    private LsmStorage lsmStorage;

    public MinBase() throws IOException {
        this.lsmStorage = new LsmStorage();
    }

    // 当前读
    public byte[] get(byte[] key) {
        return lsmStorage.get(key);
    }
    // 快照读
    public byte[] getWithSnapshot(byte[] key, long snapshot) {
        return lsmStorage.get(new Key(key, snapshot));
    }


    public long getSnapshot() {
        return lsmStorage.getSnapshot();
    }

    public void put(byte[] key, byte[] value) {
        lsmStorage.put(key, value);
    }

    public void put(WriteBatch writeBatch) {
        lsmStorage.put(writeBatch);
    }

    public boolean checkAndPut(byte[] checkKey, byte[] checkValue, byte[] key, byte[] value) {
        return lsmStorage.checkAndPut(checkKey, checkValue, key, value);
    }

    public void delete(byte[] key) {
        lsmStorage.delete(key);
    }

    public SnapshotIterator scanWithSnapshot(byte[] startKey, byte[] endKey, long snapshot) {
        return lsmStorage.scan(startKey, endKey, snapshot);
    }

    public KeyIterator scan(byte[] startKey, byte[] endKey) {
        return lsmStorage.scan(startKey, endKey);
    }


    public Transaction newTransaction() {
        return TransactionManager.newTransaction(this.lsmStorage);
    }
}
