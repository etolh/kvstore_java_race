package com.huawei.hwcloud.tarus.kvstore.test.NIOTest;

import io.netty.util.concurrent.FastThreadLocal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;

public class TestByteBuffReadAndWrite {

    private static final int VALUE_LEN = 4096;
    private static final int SHIF_NUM = 12;
    private static final int NUM = 100;

//    private ByteBuffer vBuffer = ByteBuffer.allocate(VALUE_LEN);

    // valueBuffer: 存储value
    private static FastThreadLocal<ByteBuffer> localBufferValue = new FastThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() throws Exception {
            return ByteBuffer.allocateDirect(VALUE_LEN);
        }
    };

    // 线程私有的buffer，用于byte数组转long
    private static FastThreadLocal<byte[]> localValueBytes = new FastThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() throws Exception {
            return new byte[VALUE_LEN];
        }
    };

    public void write(String fileName, byte[] value, long off) throws IOException {

        ByteBuffer vBuffer = localBufferValue.get();

        FileChannel vFileChannel = new RandomAccessFile(new File(fileName), "rw").getChannel();
        // 1. value写入到buffer
        vBuffer.put(value);
        // 2. buffer转换为读模式
        vBuffer.flip();

        // 3. buffer读出数据，写入到channel
        vFileChannel.write(vBuffer, off);

        // 4. 清空vBuffer
        vBuffer.clear();
    }

    private byte[] read(String fileName, long off) throws IOException {

        ByteBuffer vBuffer = localBufferValue.get();


        FileChannel vFileChannel = new RandomAccessFile(new File(fileName), "rw").getChannel();

        // 1. channel写入数据到buffer
        int len = vFileChannel.read(vBuffer, off);

        // 2. 转换模式
        vBuffer.flip();
//        byte[] values = new byte[len];
        byte[] values = localValueBytes.get();

        // 3. 读出到bytes
        vBuffer.get(values, 0, len);

        // 4. 清空
        vBuffer.clear();

        return values;
    }

    private byte[] readMMP(String fileName, long off) throws IOException {

        FileChannel vChannel = new RandomAccessFile(new File(fileName), "rw").getChannel();
        long len = vChannel.size();

        MappedByteBuffer mapBuffer = vChannel.map(FileChannel.MapMode.READ_WRITE, 0, len);

        byte[] bytes = localValueBytes.get();
        // get:获取len数据，放入到bytes[off...off+len]
        mapBuffer.get(bytes,0, VALUE_LEN);

        return bytes;
    }

    public static void main(String[] args) {

        String fileName = "C:\\Users\\t-liah\\Desktop\\db\\JavaPractice\\data\\1.data";
        TestByteBuffReadAndWrite tester = new TestByteBuffReadAndWrite();


        // 按序写入100个4KB数据
        byte[] values = new byte[VALUE_LEN];

        System.out.println("write init");
        long wStart = System.currentTimeMillis();
        for (int i = 1; i <= NUM; i++){
            byte ib = (byte)i;
//            System.out.println(ib);
            Arrays.fill(values,ib);
            /*
            System.out.println("value:"+new String(values));
            System.out.println("value length:"+values.length);
            for (int j =1; j < 10; j++) {
                System.out.println("value["+j+"]:"+values[j]);
            }
            */
            try {
                tester.write(fileName, values, (long)(i-1)<<SHIF_NUM);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        long wEnd = System.currentTimeMillis();
        System.out.println(String.format("write end, tim %s毫秒", wEnd - wStart));


        System.out.println("read init");
        long rStart = System.currentTimeMillis();
        for (int i = 1; i <= NUM; i++) {
            try {
                byte[] vs = tester.read(fileName, (long)(i-1)<<SHIF_NUM);
                /*
                System.out.println("vs"+vs.length);
                for (int j =1; j < 2; j++) {
                    System.out.println("vs["+j+"]:"+vs[j]);
                }
                */
                // verify
                Arrays.fill(values, (byte)i);
                if (!Arrays.equals(vs, values)) {
                    System.out.println("Error" + i);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        long rEnd = System.currentTimeMillis();
        System.out.println(String.format("read end, tim %s毫秒", rEnd - rStart));

        System.out.println("**********************************************");

    }

    private void testMMPReadAndWrite() {
        String fileName2 = "C:\\Users\\t-liah\\Desktop\\db\\JavaPractice\\data\\2.data";
        // 按序写入100个4KB数据
        byte[] values = new byte[VALUE_LEN];

        FileChannel vChannel = null;
        MappedByteBuffer mapBuffer = null;
        int len = VALUE_LEN * NUM;
        try {
            vChannel = new RandomAccessFile(new File(fileName2), "rw").getChannel();
            mapBuffer = vChannel.map(FileChannel.MapMode.READ_WRITE, 0, len);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("write map init");
        long wStart2 = System.currentTimeMillis();
        for (int i = 1; i <= NUM; i++){
            Arrays.fill(values,(byte)i);
            mapBuffer.put(values,0, VALUE_LEN);
        }
        long wEnd2 = System.currentTimeMillis();
        System.out.println(String.format("write end, tim %s毫秒", wEnd2 - wStart2));

        mapBuffer.force();
        System.out.println("force"+mapBuffer);
        mapBuffer.flip();
        System.out.println("flip"+mapBuffer);

        System.out.println("map read init");
        long rStart2 = System.currentTimeMillis();
        byte[] bytes = localValueBytes.get();
        for (int i = 1; i <= NUM; i++) {
            mapBuffer.get(bytes,0, VALUE_LEN);
            Arrays.fill(values, (byte)i);
//            System.out.println("value "+i+":"+bytes[0]);
            if (!Arrays.equals(bytes, values)) {
                System.out.println("Error" + i);
            }
        }
        long rEnd2 = System.currentTimeMillis();
        System.out.println(String.format("read end, tim %s毫秒", rEnd2 - rStart2));

        mapBuffer.clear();
        unmap(mapBuffer);
    }
    public static void unmap(final MappedByteBuffer mappedByteBuffer) {
        try {
            if (mappedByteBuffer == null) {
                return;
            }

            mappedByteBuffer.force();
            AccessController.doPrivileged(new PrivilegedAction<Object>() {
                @Override
                @SuppressWarnings("restriction")
                public Object run() {
                    try {
                        Method getCleanerMethod = mappedByteBuffer.getClass()
                                .getMethod("cleaner", new Class[0]);
                        getCleanerMethod.setAccessible(true);
                        sun.misc.Cleaner cleaner =
                                (sun.misc.Cleaner) getCleanerMethod
                                        .invoke(mappedByteBuffer, new Object[0]);
                        cleaner.clean();

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println("clean MappedByteBuffer completed");
                    return null;
                }
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}