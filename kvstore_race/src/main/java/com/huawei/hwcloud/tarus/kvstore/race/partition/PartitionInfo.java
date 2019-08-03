package com.huawei.hwcloud.tarus.kvstore.race.partition;

/**
 * 记录分区的分区号\偏移量
 */
public class PartitionInfo {

    private int par_no;
    private int offset;

    public int getPar_no() {
        return par_no;
    }

    public void setPar_no(int par_no) {
        this.par_no = par_no;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }
}
