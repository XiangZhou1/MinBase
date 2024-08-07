package org.minbase.common.utils;

import org.minbase.common.operation.ColumnValues;
import org.minbase.common.operation.Get;
import org.minbase.common.rpc.proto.generated.ClientProto;

import java.util.Map;

public class ProtobufUtil {
    public static Get toGet(ClientProto.GetRequest request) {
        String key = request.getKey();
        Get get = new Get(ByteUtil.toBytes(key));
        int columnsCount = request.getColumnsCount();
        for (int i = 0; i < columnsCount; i++) {
            final String column = request.getColumns(i);
            get.addColumn(ByteUtil.toBytes(column));
        }
        return get;
    }

    public static ClientProto.GetResponse toGetResponse(String key, ColumnValues columnValues) {
        final ClientProto.GetResponse.Builder builder = ClientProto.GetResponse.newBuilder();
        builder.setKey(key);
        int i = 0;
        for (Map.Entry<byte[], byte[]> entry : columnValues.getColumnValues().entrySet()) {
            final ClientProto.ColumnValue.Builder builder1 = ClientProto.ColumnValue.newBuilder();
            builder1.setColumn(new String(entry.getKey())).setValue(new String(entry.getValue()));
            builder.setColumnValues(i, builder1.build());
        }
        return builder.build();
    }
}
