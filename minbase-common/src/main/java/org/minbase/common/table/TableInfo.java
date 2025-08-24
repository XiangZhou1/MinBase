package org.minbase.common.table;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public class TableInfo {
    String name;
    Set<String> columns = new TreeSet<>();

    public TableInfo(String name, HashSet columns) {
        this.name = name;
        this.columns = columns;
    }

    public TableInfo(String name) {
        this.name = name;
    }
    public void addColumn(String column) {
        columns.add(column);
    }

    public String getName() {
        return name;
    }

    public Set<String> getColumns() {
        return columns;
    }

    @Override
    public String toString() {
        return "TableInfo{" +
                "name='" + name + '\'' +
                ", columns=" + columns +
                '}';
    }
}
