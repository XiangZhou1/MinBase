package org.minbase.client;

import org.junit.Test;
import org.minbase.common.exception.ServerException;
import org.minbase.common.exception.TableNotExistException;
import org.minbase.common.table.TxTable;
import org.minbase.common.table.op.ColumnValues;
import org.minbase.common.table.op.Get;
import org.minbase.common.table.op.Put;
import org.minbase.common.table.ClientTable;
import org.minbase.common.table.transaction.Transaction;
import org.minbase.common.utils.ByteUtil;

import java.nio.charset.StandardCharsets;

public class MinClientTest {
    @Test
    public void testCreateTable() {
        MinClient client = new MinClient("127.0.0.1", 4444);
        client.createTable("table");
        client.close();
    }

    @Test
    public void testPut() throws Exception {
        MinClient client = new MinClient("127.0.0.1", 4444);
        ClientTable table = client.getTable("table");
        table.put("k1", "c1", "v1");
        client.close();
    }

    @Test
    public void testGet() throws Exception {
        MinClient client = new MinClient("127.0.0.1", 4444);
        ClientTable table = client.getTable("table");
        String val = table.get("k1", "c1");
        System.out.println(val);
        client.close();
    }

    @Test
    public void testCheckAndPut() throws ServerException, TableNotExistException {
        MinClient client = new MinClient("127.0.0.1", 4444);
        ClientTable table = client.getTable("table");
        boolean success = table.checkAndPut("k1", "c1", "v1",
                "k2", "c1", "v2");
        System.out.println(success);
        client.close();
    }

    @Test
    public void testGet2() throws Exception {
        MinClient client = new MinClient("127.0.0.1", 4444);
        ClientTable table = client.getTable("table");
        String val = table.get("k2", "c1");
        System.out.println(val);
        client.close();
    }
    @Test
    public void testDelete() throws Exception {
        MinClient client = new MinClient("127.0.0.1", 4444);
        ClientTable table = client.getTable("table");
        table.delete("k2", "c1");
        client.close();
    }

    @Test
    public void testTxPut() throws Exception {
        MinClient client = new MinClient("127.0.0.1", 4444);
        Transaction transaction = client.beginTransaction();
        TxTable table = transaction.getTable("table");
        table.put("k3", "c3", "v3");
        transaction.commit();
        client.close();
    }
    @Test
    public void testTxPut3() throws Exception {
        MinClient client = new MinClient("127.0.0.1", 4444);
        Transaction transaction = client.beginTransaction();
        TxTable table = transaction.getTable("table");
        table.put("k3", "c3", "v4");
        transaction.rollback();
        client.close();
    }

    @Test
    public void testTxPut4() throws Exception {
        MinClient client = new MinClient("127.0.0.1", 4444);
        Transaction transaction = client.beginTransaction();
        TxTable table = transaction.getTable("table");
        String val = table.get("k3", "c3");
        System.out.println(val);
        transaction.commit();
        client.close();
    }
    @Test
    public void testTxCheckAndPut() throws Exception {
        MinClient client = new MinClient("127.0.0.1", 4444);
        try {
            Transaction transaction = client.beginTransaction();
            TxTable table = transaction.getTable("table");
            boolean success = table.checkAndPut("k3", "c3", "v3",
                    "k4", "c4", "v4");
            System.out.println(success);
            transaction.commit();
        } finally {
            client.close();
        }
    }

    @Test
    public void testTxDelete() throws Exception {
        MinClient client = new MinClient("127.0.0.1", 4444);
        try {
            Transaction transaction = client.beginTransaction();
            TxTable table = transaction.getTable("table");
            table.delete("k3", "c3");
            transaction.commit();
        } finally {
            client.close();
        }
    }
}
