package com.huawei.hwcloud.tarus.kvstore.race.common;

import io.netty.util.concurrent.FastThreadLocal;

import java.nio.ByteBuffer;

public class Constant {

    public static final int KEY_LEN = 8;
    public static final int VALUE_LEN = 4096;	// 4KB
    // key:8B fileNo:2B off:2B
    public static final int KEY_OFF_LEN = 16;
    public static final int SHIFT_NUM = 12;
    // 数据量
    public static final int MSG_NUMBER = 4096000;
    // 文件数量:keyFile和valueFile切分1024个分区
    public static final int PARTITION_NUM = 10;
    public static final int PARTITION_COUNT = 1 << PARTITION_NUM;
    // 多线程读取索引文件，切分为16个索引文件
//    private static int THREAD_NUM = 16;
    // 一个分区最多存储: 4000*1024>4000000
    public static final int KV_NUMBER_PER_PAR = 4000;


    // keyBuffer: 存储keyFile中(key,valueOff)值
    public static FastThreadLocal<ByteBuffer> localBufferKey = new FastThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() throws Exception {
            return ByteBuffer.allocateDirect(KEY_OFF_LEN);
        }
    };

    // valueBuffer: 存储value
    public static FastThreadLocal<ByteBuffer> localBufferValue = new FastThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() throws Exception {
            return ByteBuffer.allocateDirect(VALUE_LEN);
        }
    };

    // 线程私有的buffer，用于byte数组转long
    public static FastThreadLocal<byte[]> localValueBytes = new FastThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() throws Exception {
            return new byte[VALUE_LEN];
        }
    };
}
