package org.minbase.server.kv.storage.block;


import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.Length;
import org.minbase.server.kv.utils.Codec;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;

/**
 * 数据结构
 * ---------------------------   (< ====  offset )
 * DataBlock1     | KeyValue (byte[])    (key_len + key + valueLen + value)
 * | KeyValue (byte[])
 * | .........
 * ---------------------------
 */
public class DataBlock extends Block implements Length, Codec {
    private ArrayList<KeyValue> data;
    private int dataLength;
    private int count = 0;

    public DataBlock() {
        data = new ArrayList<>();
        dataLength = 0;
        count = 0;
    }

    public int getKeyValueCount() {
        return count;
    }

    public void setKeyValueCount(int count) {
        this.count = count;
    }

    public void add(KeyValue kv) {
        data.add(kv);
        dataLength += kv.length();
        count++;
    }


    public ArrayList<KeyValue> getData() {
        return data;
    }

    @Override
    public int length() {
        return dataLength;
    }

    @Override
    public void decode(byte[] bytes) {
        int pos = 0;
        for (int i = 0; i < count; i++) {
            KeyValue keyValue = new KeyValue();
            keyValue.decode(bytes, pos);
            pos += keyValue.length();
            data.add(i, keyValue);
            dataLength += keyValue.length();
        }
    }

    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) length()];
        int index = 0;
        for (KeyValue entry : data) {
            byte[] buf = entry.encode();
            System.arraycopy(entry.encode(), 0, bytes, index, buf.length);
            index += buf.length;
        }
        return bytes;
    }

    @Override
    public int encodeToStream(OutputStream outputStream) throws IOException {
        int index = 0;
        for (KeyValue entry : data) {
            int len = entry.encodeToStream(outputStream);
            index += len;
        }
        return index;
    }


    public void clear() {
        data.clear();
        dataLength = 0;
        count = 0;
    }

    @Override
    public String toString() {
        return "DataBlock{" +
                "dataLength=" + dataLength +
                ", num=" + count +
                '}';
    }
}
