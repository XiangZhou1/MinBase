package org.minbase.common.op;

import java.util.ArrayList;
import java.util.List;

public class Get extends Op {
    private String key;
    private List<String> columns = new ArrayList<>();

    public Get() {
    }

    public Get(String key) {
        this.key = key;
    }

    public Get(String key, List<String> columns) {
        this.key = key;
        this.columns = columns;
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
