package org.minbase.server.storage.block;



import org.minbase.server.constant.Constants;
import org.minbase.server.utils.ByteUtils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class BloomFilterBlock extends Block {
    private long bitSet = 0;
    private MessageDigest digest;

    public BloomFilterBlock() {
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    // 添加元素到过滤器
    public void add(byte[] element) {
        byte[] hash1 = hash(element);
        for (byte b : hash1) {
            int offset = (int)b;
            if(offset < 0){
                offset = -offset;
            }
            bitSet = bitSet | ((long)1<<offset);
        }
    }

    // 检查元素是否可能在过滤器中
    public boolean mightContain(byte[] element) {
        byte[] hash1 = hash(element);
        for (byte b : hash1) {
            int offset = (int)b;
            if(offset < 0){
                offset = -offset;
            }
            long result = bitSet & ((long)1<<offset);
            if (result == 0) {
                return false;
            }
        }
        return true;
    }


    @Override
    public long length() {
        return Constants.LONG_LENGTH;
    }

    // 简单的哈希函数，使用一个随机数生成器来保证哈希值的随机性
    private byte[] hash(byte[] element) {
        return this.digest.digest(element);
    }

    public byte[] encode() {
        return ByteUtils.longToByteArray(bitSet);
    }

    public void decode(byte[] buf) {
        this.bitSet = ByteUtils.byteArrayToLong(buf, 0);
    }

    @Override
    public String toString() {
        return "BloomFilterBlock{" +
                "bitSet=" + bitSet +
                ", digest=" + digest +
                '}';
    }
}
