package com.huawei.hwcloud.tarus.kvstore.race.common;

import com.huawei.hwcloud.tarus.kvstore.util.BufferUtil;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.DecimalFormat;
import java.util.Arrays;

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

    public static final String fillThreadNo(final int thread_no){
        DecimalFormat df = new DecimalFormat(THREAD_PATH_FORMAT);
        return df.format(Integer.valueOf(thread_no));
    }


    public static byte[] long2bytes(long key) {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte)(key & 0xFF);
            key >>= 8;
        }
        return result;
    }

    public static long combine(int parNo, int offset){
        long partitionOff = parNo;
        partitionOff <<= 32;
        partitionOff += offset;
        return partitionOff;
    }

    public static int[] divide(long pos){
        int[] coms = new int[2];
        coms[0] = (int)(pos & 0xFFFFFFF);
        pos >>>= 32;
        coms[1] = (int)(pos & 0xFFFFFFF);
        return coms;
    }

    public static final String buildKey(final long i) {
        return String.format("%d", i);
    }

    public static final byte[] buildVal(final int i) {
        byte[] bytes = new byte[4096];
        Arrays.fill(bytes,(byte)i);
        return bytes;
    }

    /**
     * 刷新磁盘
     */
    private void flush(FileChannel channel) throws IOException {
        if (channel != null && channel.isOpen()){
            channel.force(false);
        }
    }

    /**
     * 清理MMAP
     */
    public static void clean(MappedByteBuffer mappedByteBuffer) {
        ByteBuffer buffer = mappedByteBuffer;
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
            throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }
        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }
}
