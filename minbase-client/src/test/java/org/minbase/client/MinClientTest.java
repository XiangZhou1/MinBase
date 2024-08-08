package org.minbase.client;

import org.junit.Test;
import org.minbase.common.operation.ColumnValues;
import org.minbase.common.operation.Get;
import org.minbase.common.operation.Put;
import org.minbase.common.table.Table;
import org.minbase.common.transaction.Transaction;

import java.nio.charset.StandardCharsets;

public class MinClientTest {
    @Test
    public void test1() {
        MinClient client = new MinClient("127.0.0.1", 9876);
        Transaction transaction = client.beginTransaction();
        try {
            Table testTable = transaction.getTable("testTable");
            Put put = new Put("k1".getBytes(StandardCharsets.UTF_8));
            put.addValue("cl1".getBytes(StandardCharsets.UTF_8), "v1".getBytes(StandardCharsets.UTF_8));
            testTable.put(put);
            transaction.commit();
        } catch (Exception e) {
            transaction.rollback();
        }
        client.close();
    }

    @Test
    public void test2() {
        MinClient client = new MinClient("127.0.0.1", 9876);
        Table testTable = client.getTable("testTable");
        Get get = new Get("k1".getBytes(StandardCharsets.UTF_8));

        ColumnValues columnValues = testTable.get(get);

        client.close();
    }
}
