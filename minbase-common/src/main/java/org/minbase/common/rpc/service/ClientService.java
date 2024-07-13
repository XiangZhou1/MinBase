package org.minbase.common.rpc.service;

public interface ClientService {
    // 当前读
    String get(String key);
    void put(String key, String value);
    boolean checkAndPut(String checkKey, String checkValue, String key, String value);
    void delete(String key);
}
