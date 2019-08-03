package com.huawei.hwcloud.tarus.kvstore.race.index.map;

/**
 * KeyFile中key与(par_no,offset)映射接口文件
 * 实现方式：
 * HPPC-HashMap
 * Array:有序数组，二分查找
 * Tree
 */
public interface MemoryMap {
    int size();
    long get(long key);
    void insert(long key, long parInfo);
}
