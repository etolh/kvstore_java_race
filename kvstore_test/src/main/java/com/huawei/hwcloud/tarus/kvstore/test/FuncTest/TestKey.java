package com.huawei.hwcloud.tarus.kvstore.test.FuncTest;


import io.netty.util.concurrent.FastThreadLocal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class TestKey {

    private static final String DEFAULT_ENCODING = "UTF-8";

    public void testString2long() {

//        String sKey = "9",  eKey = "10";
        String sKey = "188978610969",  eKey = "188978610970";
//        String sKey = "69",  eKey = "70";
        System.out.println(stringToBytes(sKey).length);
        System.out.println(stringToBytes(eKey).length);

        System.out.println(stringToBytes("0")[0]);
        long sNumKey = bytes2long(stringToBytes(sKey));
//        long eNumKey = bytes2long(stringToBytes(eKey));
        System.out.println(sNumKey);
//        System.out.println(eNumKey);

        long nS = Long.parseLong(sKey), nE = Long.parseLong(eKey);
        System.out.println(nS);
        System.out.println(nE);
    }

    private int hash1(long key) {
        return (int)(key & 0x3F);
    }

    private int hash2(long key) {
        return (int)(key % 64);
    }

    public static long bytes2long(byte[] bytes) {
        long result = 0;
        for (int i = 0; i < bytes.length; i++) {
//            System.out.println("byte"+bytes[i]);
            result *= 10;
//            result <<= 8;
//            result |= (bytes[i] & 0xFF);
            System.out.println("mins"+(bytes[i]-stringToBytes("0")[0]));
            result += (bytes[i] - stringToBytes("0")[0]);
        }
        return result;
    }

    public static final byte[] stringToBytes(final String str) {
        try {
            return str.getBytes(DEFAULT_ENCODING);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static final int VALUE_LEN = 4096;	// 4KB
    private static FastThreadLocal<ByteBuffer> localBufferValue = new FastThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() throws Exception {
            return ByteBuffer.allocateDirect(VALUE_LEN);
        }
    };

    public void testSetValue(String dir) {
        int offset = 0;
        try {
            File dataFile = new File(dir + File.separator + "1.data");
            RandomAccessFile rf = new RandomAccessFile(dataFile, "rw");
            FileChannel vChannel = rf.getChannel();

            byte[] bytes = new byte[4096];
            for (int i = 5; i < VALUE_LEN; i++)
                bytes[i] = '1';
            for(int i = 1; i <=5; i++) {
                String value = "key" + i;
                System.arraycopy(value.getBytes(), 0, bytes, 0, value.getBytes().length);

                ByteBuffer valueBuffer = localBufferValue.get();
                valueBuffer.clear();
                valueBuffer.put(bytes);
                valueBuffer.flip();
                System.out.println(valueBuffer);
                vChannel.write(valueBuffer, (long)(offset<<12));
                System.out.println("off"+offset+" pos"+(offset<<12));
                offset++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void testGetValue(String dir) {
        int offset = 0;
        try {
            File dataFile = new File(dir + File.separator + "1.data");
            RandomAccessFile rf = new RandomAccessFile(dataFile, "rw");
            FileChannel vChannel = rf.getChannel();

            byte[] bytes = new byte[4096];
            for(int i = 1; i <=5; i++) {

                ByteBuffer valurBuffer = localBufferValue.get();
                // 从valueFile中读取
                vChannel.read(valurBuffer, (long)(offset << 12));
                valurBuffer.flip();
                valurBuffer.get(bytes, 0, VALUE_LEN);
                valurBuffer.clear();
                // 写入到value
                offset++;
                System.out.println(bytes.length);
                System.out.println(new String(bytes));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
//        new TestKey().testString2long();
        String dir = "C:\\Users\\t-liah\\Desktop\\db\\JavaPractice\\data";
//        new TestKey().testSetValue(dir);
        new TestKey().testGetValue(dir);
    }

}
