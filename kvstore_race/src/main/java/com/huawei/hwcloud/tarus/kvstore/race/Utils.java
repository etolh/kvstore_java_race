package com.huawei.hwcloud.tarus.kvstore.race;

import com.huawei.hwcloud.tarus.kvstore.util.BufferUtil;

import java.text.DecimalFormat;

/**
 *  Utils
 */
public class Utils {

    // 不同线程线程名前缀 key文件 01_kv_store.key value文件 01_1.data
    private static final byte base = BufferUtil.stringToBytes("0")[0];
    private static final String THREAD_PATH_FORMAT = "0000";



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

    // value file hash 64个文件,取余或0x 2^6
    public static int valueFileHash(long key) {
        return (int)(key & 0x3F);
    }

    // keyFile Hash 16线程 2^4
    public static int keyFileHash(long key) {
        return (int)(key & 0x0F);
    }

    // 保留key前6位
    public static int valueFileHash2(long key) {
        return (int)(key >>> 58);
    }

    //
    public static int keyFileHash2(long key) {
        return (int)(key >>> 60);
    }

    // 取key前10位，一致的在统一文件中
    public static int fileHash(long key) {
        return (int)(key >>> 54);
    }

    // 取key后10位（后10位为1 ）
    public static int fileHash2(long key) {
        return (int)(key & 0x003FF);
    }

    public static int getPartition(byte[] key) {
        return ((key[0] & 0xff) << 2) | ((key[1] & 0xff) >> 6);
    }

    public static final String fillThreadNo(final int no){
        DecimalFormat df = new DecimalFormat(THREAD_PATH_FORMAT);
        return df.format(Integer.valueOf(no));
    }


    public static byte[] long2bytes(long key) {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte)(key & 0xFF);
            key >>= 8;
        }
        return result;
    }

}
