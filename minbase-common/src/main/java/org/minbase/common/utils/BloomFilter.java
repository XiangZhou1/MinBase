package org.minbase.common.utils;


import java.util.Arrays;
import java.util.BitSet;

/**
 * 一个自定义的布隆过滤器实现
 */
public class BloomFilter {

    /**
     * 位数组，用于存储元素信息
     */
    protected BitSet bitSet;

    /**
     * 位数组的大小 (m)
     */
    protected int bitSetSize;

    /**
     * 哈希函数的数量 (k)
     */
    protected int numberOfHashFunctions;

    /**
     * 当前已添加的元素数量
     */
    protected int elementCount;

    /**
     * 构造一个布隆过滤器
     *
     * @param expectedInsertions       预计要插入的元素数量 (n)
     * @param falsePositiveProbability 期望的误判率 (p)
     */
    public BloomFilter(int expectedInsertions, double falsePositiveProbability) {
        // 根据期望的插入量和误判率，计算最佳的位数组大小 (m) 和哈希函数数量 (k)
        // 公式来源: https://en.wikipedia.org/wiki/Bloom_filter
        // m = - (n * ln(p)) / (ln(2)^2)
        this.bitSetSize = (int) Math.ceil(-(expectedInsertions * Math.log(falsePositiveProbability)) / (Math.log(2) * Math.log(2)));
        // k = (m / n) * ln(2)
        this.numberOfHashFunctions = (int) Math.max(1, Math.round((double) bitSetSize / expectedInsertions * Math.log(2)));

        this.bitSet = new BitSet(this.bitSetSize);
        this.elementCount = 0;
    }

    /**
     * 向布隆过滤器中添加一个元素
     *
     * @param element 要添加的元素
     */
    public void add(byte[] element) {
        int[] hashIndices = getHashIndices(element);
        for (int index : hashIndices) {
            bitSet.set(index, true);
        }
        elementCount++;
    }

    /**
     * 检查一个元素是否可能存在于布隆过滤器中
     *
     * @param element 要检查的元素
     * @return 如果元素可能存在，返回 true；如果元素绝对不存在，返回 false
     */
    public boolean contains(byte[] element) {
        int[] hashIndices = getHashIndices(element);
        for (int index : hashIndices) {
            if (!bitSet.get(index)) {
                // 只要有一个位是0，就说明元素绝对不存在
                return false;
            }
        }
        // 所有位都为1，说明元素可能存在
        return true;
    }

    /**
     * 获取元素对应的 k 个哈希位置
     * <p>
     * 这是一个常见的技巧：使用两个哈希函数 h1 和 h2 来模拟 k 个哈希函数
     * h_i(x) = (h1(x) + i * h2(x)) % m
     *
     * @param element 元素
     * @return 包含 k 个哈希索引的数组
     */
    private int[] getHashIndices(byte[] element) {
        int[] indices = new int[numberOfHashFunctions];

        // 使用对象自带的 hashCode() 作为第一个哈希函数
        int hash1 = Arrays.hashCode(element);

        // 使用第一个哈希值的高位和低位异或来创建第二个哈希值，增加随机性
        int hash2 = hash1 >>> 16;

        for (int i = 0; i < numberOfHashFunctions; i++) {
            int combinedHash = hash1 + i * hash2;
            // 确保哈希值为正数
            if (combinedHash < 0) {
                combinedHash = ~combinedHash; // 按位取反，比 Math.abs() 更安全高效
            }
            indices[i] = combinedHash % bitSetSize;
        }
        return indices;
    }

    /**
     * 获取位数组的大小
     */
    public int getBitSetSize() {
        return bitSetSize;
    }

    /**
     * 获取哈希函数的数量
     */
    public int getNumberOfHashFunctions() {
        return numberOfHashFunctions;
    }

    /**
     * 获取当前已添加的元素数量
     */
    public int getElementCount() {
        return elementCount;
    }
}