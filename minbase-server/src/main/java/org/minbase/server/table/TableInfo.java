package org.minbase.server.table;

import java.util.ArrayList;
import java.util.List;

public class TableInfo {
    private String name;
    private List<String> columns;

    public TableInfo() {
    }

    public TableInfo(String name) {
        this.name = name;
        this.columns = new ArrayList<>();
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

    public void addColumn(String column) {
        this.columns.add(column);
    }
}
