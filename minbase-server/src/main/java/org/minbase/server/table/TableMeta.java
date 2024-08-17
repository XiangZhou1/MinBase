package org.minbase.server.table;

import org.minbase.common.utils.ByteUtil;
import org.minbase.server.constant.Constants;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class TableMeta {
    private String name;
    private List<String> columns;

    public TableMeta() {
    }

    public TableMeta(String tableName, List<String> columns) {
        this.name = tableName;
        this.columns = columns;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    /*
        |len(name)|name|size(columns)|len(column1)|column1|...........
     */
    public byte[] encode() {
        return null;
    }

    public int encodeToFile(OutputStream outputStream) throws IOException {
        outputStream.write(name.length());
        outputStream.write(ByteUtil.toBytes(name));
        outputStream.write(columns.size());
        for (String column : columns) {
            outputStream.write(column.length());
            outputStream.write(ByteUtil.toBytes(column));
        }
        return length();
    }

    public void decode(byte[] buf) {
        int pos = 0;
        int nameLen =  ByteUtil.byteArrayToInt(buf, pos);
        pos += Constants.INTEGER_LENGTH;
        byte[] nameBytes= new byte[nameLen];
        System.arraycopy(buf, pos, nameBytes, 0, nameLen);
        name = new String(nameBytes);
        pos += nameLen;

        int columnSize = ByteUtil.byteArrayToInt(buf, pos);
        pos += Constants.INTEGER_LENGTH;

        columns = new ArrayList<>();
        for(int i=0; i<columnSize; i++){
            int columnLen =  ByteUtil.byteArrayToInt(buf, pos);
            pos += Constants.INTEGER_LENGTH;
            byte[] columnBytes= new byte[columnLen];
            System.arraycopy(buf, pos, columnBytes, 0, columnLen);
            columns.add(new String(columnBytes));
            pos += columnLen;
        }
    }

    public int length() {
        int len = 0;
        len += Constants.INTEGER_LENGTH;
        len += name.length();
        len += Constants.INTEGER_LENGTH;
        for (String column : columns) {
            len += Constants.INTEGER_LENGTH;
            len += column.length();
        }
        return len;
    }
}
