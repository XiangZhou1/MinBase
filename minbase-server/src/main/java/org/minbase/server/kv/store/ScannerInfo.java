package org.minbase.server.kv.store;

import java.util.concurrent.atomic.AtomicLong;

public class ScannerInfo {
    public static AtomicLong SCANNER_ID = new AtomicLong(0);
    private long scannerId;
    private long readPoint;

    public ScannerInfo(long scannerId, long readPoint) {
        this.scannerId = scannerId;
        this.readPoint = readPoint;
    }

    public long getScannerId() {
        return scannerId;
    }

    public void setScannerId(long scannerId) {
        this.scannerId = scannerId;
    }

    public long getReadPoint() {
        return readPoint;
    }

    public void setReadPoint(long readPoint) {
        this.readPoint = readPoint;
    }
}
