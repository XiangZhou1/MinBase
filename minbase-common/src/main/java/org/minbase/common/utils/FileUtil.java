package org.minbase.common.utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileUtil {

    public static byte[] read(RandomAccessFile randomAccessFile, long bufSize) throws IOException {
        byte[] buf = new byte[(int) bufSize];
        int pos = 0;
        while (pos < bufSize) {
            int read = randomAccessFile.read(buf, pos, (int) bufSize - pos);
            pos += read;
        }
        return buf;
    }

    public static void rename(File file, File newFile) throws IOException {
        if (!file.renameTo(newFile)) {
            throw new IOException("Rename file failed, fileName=" + file.getName());
        }
    }
}
