package com.huawei.hwcloud.tarus.kvstore.race.index.map;

import com.carrotsearch.hppc.LongLongHashMap;

/**
 * 使用HPPC HashMap存储key与(par_no, offset)映射
 */
public class HPPCMemoryMap implements MemoryMap {

    private LongLongHashMap indexMap;
    public HPPCMemoryMap(int expectedElements, double loadFactor){
        indexMap = new LongLongHashMap(expectedElements, loadFactor);
    }

    @Override
    public int size() {
        return indexMap.size();
    }

    @Override
    public long get(long numKey) {
        // 不存在返回-1
        return indexMap.getOrDefault(numKey, -1);
    }

    @Override
    public void insert(long key, long parInfo) {
        indexMap.put(key, parInfo);
    }
}
