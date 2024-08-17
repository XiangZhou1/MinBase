package org.minbase.server.storage.block;



import org.minbase.server.kv.KeyValue;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;

/**
 * 数据结构
 * ---------------------------   (< ====  offset )
 * DataBlock1     | KeyValue (byte[])    (key_len + key + valueLen + value)
 *                | KeyValue (byte[])
 *                | .........
 * ---------------------------
 */
public class DataBlock extends Block {
    private ArrayList<KeyValue> data;
    private long dataLength;
    private int num = 0;

    public DataBlock() {
        data = new ArrayList<>();
        dataLength = 0;
        num = 0;
    }

    public int getKeyValueNum() {
        return num;
    }

    public void setKeyValueNum(int num) {
        this.num = num;
    }

    public void add(KeyValue kv){
        data.add(kv);
        dataLength += kv.length();
        num ++;
    }


    public ArrayList<KeyValue> getData() {
        return data;
    }

    public long length() {
        return dataLength;
    }

    public void decode(byte[] bytes) {
        int pos = 0;
        for (int i = 0; i < num; i++) {
            KeyValue keyValue = new KeyValue();
            keyValue.decode(bytes, pos);
            pos += keyValue.length();
            data.add(i,keyValue);
            dataLength += keyValue.length();
        }
    }

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

    public int encodeToFile(OutputStream outputStream) throws IOException {
        int index = 0;
        for (KeyValue entry : data) {
            int len = entry.encodeToFile(outputStream);
            index += len;
        }
        return index;
    }


    public void clear() {
        data.clear();
        dataLength = 0;
        num = 0;
    }

    @Override
    public String toString() {
        return "DataBlock{" +
                "dataLength=" + dataLength +
                ", num=" + num +
                '}';
    }
}
