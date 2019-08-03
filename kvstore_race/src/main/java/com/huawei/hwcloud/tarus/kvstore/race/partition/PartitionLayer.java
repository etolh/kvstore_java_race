package com.huawei.hwcloud.tarus.kvstore.race.partition;

public interface PartitionLayer {
    int getPartition(long numKey);
}
