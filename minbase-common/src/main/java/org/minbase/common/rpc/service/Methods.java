package org.minbase.common.rpc.service;

public enum Methods {
    GET("get"),
    PUT("put");

    private String name;
    Methods(String name) {
        this.name = name;
    }
    public String getName() {
        return name;
    }
}
