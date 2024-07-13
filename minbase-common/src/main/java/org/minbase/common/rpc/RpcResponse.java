package org.minbase.common.rpc;

import com.alibaba.fastjson2.JSON;

import java.nio.charset.StandardCharsets;

public class RpcResponse{
    long id;
    byte code;
    Object value;

    public RpcResponse(long id, byte code) {
        this.id = id;
        this.code = code;
    }

    public RpcResponse(long id, byte code, Object value) {
        this.id = id;
        this.code = code;
        this.value = value;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public byte getCode() {
        return code;
    }

    public void setCode(byte code) {
        this.code = code;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "RpcResponse{" +
                "id=" + id +
                ", code=" + code +
                ", value=" + value +
                '}';
    }

    ///////////////////////////////////////////////////////////////////////
    // 序列化
    public byte[] serialize() {
        return JSON.toJSONString(this).getBytes(StandardCharsets.UTF_8);
    }

    public static RpcResponse deSerialize(byte[] bytes) {
        final RpcResponse rpcResponse = JSON.parseObject(bytes, RpcResponse.class);
        return rpcResponse;
    }
}
