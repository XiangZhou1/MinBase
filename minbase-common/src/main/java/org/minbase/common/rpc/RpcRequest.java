package org.minbase.common.rpc;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;


import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class RpcRequest {
    long id;
    String methodName;
    Object[] args;

    public RpcRequest(long id, String methodName, Object[] args) {
        this.id = id;
        this.methodName = methodName;
        this.args = args;
    }


    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public Object[] getArgs() {
        return args;
    }

    public void setArgs(Object[] args) {
        this.args = args;
    }

    @Override
    public String toString() {
        return "RpcRequest{" +
                "id=" + id +
                ", methodName='" + methodName + '\'' +
                ", args=" + Arrays.toString(args) +
                '}';
    }

    ///////////////////////////////////////////////////////////////////////
    // 序列化

    public byte[] serialize() {
        return JSON.toJSONString(this).getBytes(StandardCharsets.UTF_8);
    }

    public static RpcRequest deSerialize(byte[] bytes) {
        final RpcRequest rpcRequest = JSON.parseObject(bytes, RpcRequest.class);
        return rpcRequest;
    }

}
