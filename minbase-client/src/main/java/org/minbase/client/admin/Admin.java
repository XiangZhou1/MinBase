package org.minbase.client.admin;

import java.util.List;

public interface Admin {
    boolean createTable(String tableName, List<String> columns);
    boolean addColumn(String tableName, List<String> columns);
    boolean dropTable(String tableName);
    List<String> getTableInfo(String tableName);
}
