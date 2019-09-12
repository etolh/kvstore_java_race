package com.huawei.hwcloud.tarus.kvstore.test.NIOTest;

import io.netty.util.concurrent.FastThreadLocal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class TestRandomAccessFile {

    private static FastThreadLocal<ByteBuffer> localBufferKey = new FastThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() throws Exception {
            return ByteBuffer.allocateDirect(12);
        }
    };

    public void testCreateFile(String dir){

        File dirFile = new File(dir);
        RandomAccessFile rf;
        try {
            rf = new RandomAccessFile(dirFile.getParent()+ File.separator+"1.data", "rw");
            rf.writeDouble(10);
            rf.writeDouble(10);
            rf.writeDouble(10);

            // 24 num*Double.size
            System.out.println(rf.length());


            rf = new RandomAccessFile("1.data", "r");
            for (int i = 0; i < 3; i++) {
                System.out.println("Value " + i + ": " + rf.readDouble());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void testBuffer(String dir) {
        ByteBuffer keyBuffer = localBufferKey.get();
        long k = 10;
        int off = 5;
        keyBuffer.putLong( k).putInt(off);
        keyBuffer.flip();

//        System.out.println(keyBuffer.getLong());
//        System.out.println(keyBuffer.getInt());
//        System.out.println(keyBuffer);

        System.out.println(localBufferKey.get());

        File dirFile = new File(dir);
        FileChannel rfc;
        try {
            rfc = new RandomAccessFile(new File(dirFile.getParent()+ File.separator+"1.data"), "rw").getChannel();
            rfc.write(localBufferKey.get());

            ByteBuffer bf2 = localBufferKey.get();
            rfc.read(bf2);
            bf2.flip();
            System.out.println(bf2.getLong());
            System.out.println(bf2.getInt());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void testKey(String dir) {

        int times = 4000000;

        /*
        for (int i = 0; i < times; i ++) {
            String key = buildKey(i+1);
            System.out.println(key);
            System.out.println(key);
        }
        */
        String key100000 = buildKey(times);
        byte[] keyBytes = stringToBytes(key100000);

        System.out.println(key100000);
        System.out.println(key100000.getBytes().length);

        for (int i = 0; i < keyBytes.length; i++)
            System.out.println(keyBytes[i]);


//        String key = buildKey(1);
//        byte[] keyBytes = stringToBytes(key);
//        System.out.println(keyBytes.length);

//        for (int i = 0; i < keyBytes.length; i++)
//            System.out.println((int)keyBytes[i]);
//
//        String key2 = buildKey(2);
//        byte[] keyBytes2 = stringToBytes(key2);
//
//        for (int i = 0; i < keyBytes2.length; i++)
//            System.out.println((int)keyBytes2[i]);
//        ByteBuffer keyBuffer = localBufferKey.get();
//        int off = 5;
//        System.out.println(keyBuffer);
//        keyBuffer.put(keyBytes, 0, 8);
//        System.out.println(keyBuffer);
//        keyBuffer.putInt(off);
//        System.out.println(keyBuffer);
//
//        keyBuffer.flip();
//
//        System.out.println(keyBuffer);
//
//        File dirFile = new File(dir);
//        FileChannel rfc;
//        try {
//            rfc = new RandomAccessFile(new File(dirFile.getParent()+ File.separator+"2.data"), "rw").getChannel();
//            // 写入到data
//            rfc.write(localBufferKey.get());
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    public void testString2long() {

        int times = 4000000;
//        int times = 1000;
        /*
        String key100000 = buildKey(times);
        byte[] keyBytes = stringToBytes(key100000);

        System.out.println(key100000);
        System.out.println(key100000.getBytes().length);

        long numkey = bytes2long(keyBytes);
        System.out.println("long key:"+numkey);
        */
        for (int i = 3900000; i < times; i ++) {
            String key = buildKey(i+1);
            System.out.println(key);
            long numkey = bytes2long(stringToBytes(key));
            System.out.println("long key:"+numkey);

        }
//        499041447984 t1000
//        8373370590713622576  t4000000

    }

    private static final String DEFAULT_ENCODING = "UTF-8";

    public static final byte[] stringToBytes(final String str){
        try {
            return str.getBytes(DEFAULT_ENCODING);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }


    private final String buildKey(final int i) {
        return String.format("t%d", i);
    }

    public static long bytes2long(byte[] bytes) {
        long result = 0;
        for (int i = 0; i < bytes.length; i++) {
            result <<= 8;
            result |= (bytes[i] & 0xFF);
        }
        return result;
    }

    public static void main(String[] args) {
        String dir = "C:\\Users\\t-liah\\Desktop\\db\\JavaPractice\\data";
//        new TestRandomAccessFile().testCreateFile(dir);
//        new TestRandomAccessFile().testBuffer(dir);
//        new TestRandomAccessFile().testKey(dir);
        new TestRandomAccessFile().testString2long();
    }
}

