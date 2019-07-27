package com.huawei.hwcloud.tarus.kvstore.race;

import com.huawei.hwcloud.tarus.kvstore.util.BufferUtil;

import java.text.DecimalFormat;

/**
 *  Utils
 */
public class Utils {

    // 不同线程线程名前缀 key文件 01_kv_store.key value文件 01_1.data
    private static final byte base = BufferUtil.stringToBytes("0")[0];
    private static final String THREAD_PATH_FORMAT = "00";



    // key: byte[]-->long
    public static long bytes2long2(byte[] bytes) {
        long result = 0;
        for (int i = 0; i < bytes.length; i++) {
            result <<= 8;
            result |= (bytes[i] & 0xFF);
        }
        return result;
    }

    public static long bytes2long(byte[] bytes) {
        long result = 0;
        for (int i = 0; i < bytes.length; i++) {
            result *= 10;
            result += (bytes[i] - base);
        }
        return result;
    }

    // value file hash 64个文件,取余或0x
    public static int valueFileHash(long key) {
        return (int)(key & 0x3F);
    }

    // keyFile Hash
    public static int keyFileHash(long key) {
        return (int)(key & 0x0F);
    }

    public static final String fillThreadNo(final int no){
        DecimalFormat df = new DecimalFormat(THREAD_PATH_FORMAT);
        return df.format(Integer.valueOf(no));
    }


}
