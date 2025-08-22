package org.minbase.table;

import org.junit.Test;
import org.minbase.common.table.op.Get;
import org.minbase.common.table.op.Put;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.conf.Configuration;
import org.minbase.server.table.TableManager;

public class TableManagerTest {

    @Test
    public void test1() throws Exception {
        TableManager tableManager = new TableManager(new Configuration());
    }

    @Test
    public void testCreateTable() throws Exception {
        TableManager tableManager = new TableManager(new Configuration());
        tableManager.createTable("table");
    }

    @Test
    public void testCreate() throws Exception {
        TableManager tableManager = new TableManager(new Configuration());
        tableManager.createTable("table");
        Put put = new Put(ByteUtil.toBytes("key1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v1"));
        tableManager.put("table", put);
        tableManager.get("table", new Get(ByteUtil.toBytes("key1")));
    }

    @Test
    public void testCreate2() throws Exception {
        TableManager tableManager = new TableManager(new Configuration());
        tableManager.createTable("table");
        Put put = new Put(ByteUtil.toBytes("key1"), ByteUtil.toBytes("c1"), ByteUtil.toBytes("v1"));
        tableManager.put("table", put);
        tableManager.get("table", new Get(ByteUtil.toBytes("key1")));
    }
}
