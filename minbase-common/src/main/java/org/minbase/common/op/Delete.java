package org.minbase.common.op;

import java.util.ArrayList;
import java.util.List;

public class Delete extends Op {
    private String key;
    private List<String> columns = new ArrayList<>();

    public Delete() {
    }

    public Delete(String key) {
        this.key = key;
    }

    public Delete(String key, List<String> columnValues) {
        this.key = key;
        this.columns = columnValues;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public void addColumn(String column) {
        columns.add(column);
    }

    public List<String> getColumns() {
        return columns;
    }
}
