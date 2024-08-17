package org.minbase.server.wal;

import org.minbase.common.utils.ByteUtil;
import org.minbase.server.constant.Constants;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.transaction.store.WriteBatch;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 可以把多个KeyValue记录成一条日志, 以保证多个操作的原子性
 * |length|KV|KV|...|
 */
public class LogEntry {
    private int length;
    private  WriteBatch writeBatch;
    private long sequencdId;

    /**
     *|length|tableNum|len(table1)|table1|size(kv)|len(kv1)|kv|
     * @param writeBatch
     */
    public LogEntry(WriteBatch writeBatch) {
        length += Constants.INTEGER_LENGTH;
        Set<Map.Entry<String, List<KeyValue>>> entries = writeBatch.getKeyValues().entrySet();
        length += Constants.INTEGER_LENGTH;
        for (Map.Entry<String, List<KeyValue>> entry : entries) {
            length += Constants.INTEGER_LENGTH;
            String tableName = entry.getKey();
            length += tableName.length();
            length += Constants.INTEGER_LENGTH;
            for (KeyValue keyValue : entry.getValue()) {
                length += keyValue.length();
            }
            sequencdId = entry.getValue().get(0).getKey().getSequenceId();
        }
    }

    public LogEntry() {
    }

    public int length() {
        return length;
    }

    /**
     *|length|tableNum|len(table1)|table1|size(kv)|len(kv1)|kv|
     */
    public int encodeToFile(OutputStream outputStream) throws IOException {
        outputStream.write(length);
        Set<Map.Entry<String, List<KeyValue>>> entries = writeBatch.getKeyValues().entrySet();
        outputStream.write(entries.size());
        for (Map.Entry<String, List<KeyValue>> entry : entries) {
            String tableName = entry.getKey();
            outputStream.write(tableName.length());
            outputStream.write(tableName.getBytes(StandardCharsets.UTF_8));
            for (KeyValue keyValue : entry.getValue()) {
                keyValue.encodeToFile(outputStream);
            }
        }
        return length;
    }


    /**
     *|---length---|tableNum|len(table1)|table1|size(kv)|len(kv1)|kv|
     */
    public void decode(byte[] buf) {
        writeBatch = new WriteBatch();

        int pos = 0;
        int tableNum = ByteUtil.byteArrayToInt(buf, pos);
        pos += Constants.INTEGER_LENGTH;

        for(int i=0; i<tableNum; i++){
            int tableNameLen = ByteUtil.byteArrayToInt(buf, pos);
            pos += Constants.INTEGER_LENGTH;

            byte[] tableNameBytes = new byte[tableNameLen];
            System.arraycopy(buf, pos, tableNameBytes, 0, tableNameLen);
            pos += tableNameLen;
            String tableName = new String(tableNameBytes);

            int kvNum = ByteUtil.byteArrayToInt(buf, pos);
            pos += Constants.INTEGER_LENGTH;

            for (int j = 0; j < kvNum; j++) {
                KeyValue keyValue = new KeyValue();
                keyValue.decode(buf, pos);
                pos += keyValue.length();
                writeBatch.add(tableName, keyValue);
            }
         }
    }

    public WriteBatch getWriteBatch() {
        return writeBatch;
    }

    public long getSequenceId() {
        return sequencdId;
    }
}
