package org.minbase.server.utils;

import java.io.IOException;
import java.io.RandomAccessFile;

public class IOUtils {

    public static byte[] read(RandomAccessFile randomAccessFile, long bufSize) throws IOException {
        byte[] buf = new byte[(int)bufSize];
        int pos = 0;
        while (pos < bufSize) {
            int read = randomAccessFile.read(buf, pos, (int) bufSize-pos);
            pos += read;
        }
        return buf;
    }
}
