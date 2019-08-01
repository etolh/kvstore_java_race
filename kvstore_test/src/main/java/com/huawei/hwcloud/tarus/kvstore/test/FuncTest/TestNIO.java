package com.huawei.hwcloud.tarus.kvstore.test.FuncTest;

import com.huawei.hwcloud.tarus.kvstore.race.common.Constant;
import com.huawei.hwcloud.tarus.kvstore.race.common.Utils;
import moe.cnkirito.kdio.DirectIOLib;
import moe.cnkirito.kdio.DirectIOUtils;
import moe.cnkirito.kdio.DirectRandomAccessFile;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * @ClassName TestNIO
 * @Description TODO
 * @Author lianghu
 * @Date 2019-07-27 21:47
 * @VERSION 1.0
 */
public class TestNIO {

    /**
     * 功能一致
     */
    @Test
    public void testRandomAF() {
        String path = "/Users/lianghu/Desktop/Program/kvstore_java_race/kvstore_race/src/main/java/com/huawei/hwcloud/tarus/kvstore/race/EngineKVStoreRace.java";
        try {
            RandomAccessFile file = new RandomAccessFile(path, "rw");
            FileChannel channel = file.getChannel();
            System.out.println(file.length());
            System.out.println(channel.size());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void test() {
        int f = 2;
        String path = "/Users/lianghu/Desktop/Program/kvstore_java_race/kvstore_race/src/main/java/com/huawei/hwcloud/tarus/kvstore/race/EngineKVStoreRace.java";
        File file = new File(path);
        String s = file.getPath() + File.separator + f;
        System.out.println(s);
    }

    @Test
    public void testFileChannelWrite() {
        String path = "test.data";
        try {
            RandomAccessFile file = new RandomAccessFile(path, "rw");
            FileChannel channel = file.getChannel();

            int len = channel.write(ByteBuffer.wrap("ABCD".getBytes()));
            System.out.println(len);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testBufferGet(){
        ByteBuffer buffer = ByteBuffer.allocate(4);
        byte[] bytes = "ABCD".getBytes();
        System.out.println(bytes.length);
        buffer.get(bytes, 0, -1);
    }

    /**
     * 测试DirectIO读写下4*1KB数据的时间，与FileChannel对比
     */
    @Test
    public void testDirectIO() {
        DirectIOLib directIOLib  = DirectIOLib.getLibForPath("/");
        final  int BLOCK_SZIE = 4096;
        String path = "/Users/lianghu/Desktop/Program/kvstore_java_race/kvstore_test/src/main/java/com/huawei/hwcloud/tarus/kvstore/test/FuncTest/1.data";
        long startDIO = System.currentTimeMillis();

        if (directIOLib.binit) {
            System.out.println("DIO begin");
            ByteBuffer byteBuffer = DirectIOUtils.allocateForDirectIO(directIOLib, 4 * BLOCK_SZIE);
            try {
                DirectRandomAccessFile directRandomAccessFile = new DirectRandomAccessFile(new File(path), "rw");
                // write
                for (int i = 1; i <= 4; i++) {
                    byteBuffer.clear();
                    System.out.println("before"+byteBuffer);
                    byteBuffer.put(Utils.buildVal(i));
                    byteBuffer.flip();
                    System.out.println("after"+byteBuffer);
                    directRandomAccessFile.write(byteBuffer, (i-1) * BLOCK_SZIE);
                }


                // read
                for (int i = 1; i <= 4; i++) {
                    byteBuffer.clear();
                    directRandomAccessFile.read(byteBuffer, (i-1)*BLOCK_SZIE);

                    byteBuffer.flip();
                    byte[] vals = new byte[BLOCK_SZIE];
                    byteBuffer.get(vals, 0, BLOCK_SZIE);

                    byte[] orgVals = Utils.buildVal(i);
                    if (Arrays.equals(vals, orgVals)) {
                        System.out.println("equal" + i);
                    } else {
                        System.out.println("error" + i);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        long endDIO = System.currentTimeMillis();
        System.out.println("DIO times:"+String.valueOf(endDIO-startDIO));
    }


    /**
     * 测试写4个4KB数据后才落盘
     */
    @Test
    public void TestFourBufferWrite() {
        int bufferPosition = 0;
        int times = 102;
        int count = 0;
        String path = "/Users/lianghu/Desktop/Program/kvstore_java_race/kvstore_test/src/main/java/com/huawei/hwcloud/tarus/kvstore/test/FuncTest";

        RandomAccessFile vFile;
        FileChannel fileChannel = null;
        try {
            vFile = new RandomAccessFile(path + File.separator + "test.data", "rw");
            fileChannel = vFile.getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        ByteBuffer buffer = ByteBuffer.allocate(4 * Constant.VALUE_LEN);
        long begin1 = System.currentTimeMillis();
        for (int i = 1; i <= times; i++) {
            byte[] vals = Utils.buildVal(i);
//            buffer.position(bufferPosition * Constant.VALUE_LEN);
            buffer.put(vals);
            bufferPosition++;
            if (bufferPosition >= 4){
                // flip
                buffer.position(0);
                buffer.limit(bufferPosition + Constant.VALUE_LEN);
                try {
                    fileChannel.write(buffer, (i-1)*Constant.VALUE_LEN);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                bufferPosition = 0;
                buffer.clear();
            }
        }

        long end1 = System.currentTimeMillis();
        System.out.println("times:"+(end1-begin1));


        try {
            vFile = new RandomAccessFile(path + File.separator + "test2.data", "rw");
            fileChannel = vFile.getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        ByteBuffer vbuffer = ByteBuffer.allocate(Constant.VALUE_LEN);
        long begin2 = System.currentTimeMillis();
        for (int i = 1; i <= times; i++) {
            byte[] vals = Utils.buildVal(i);
            vbuffer.put(vals);
            vbuffer.flip();
            try {
                fileChannel.write(vbuffer, (i-1)*Constant.VALUE_LEN);
            } catch (IOException e) {
                e.printStackTrace();
            }
            vbuffer.clear();
        }
        long end2 = System.currentTimeMillis();
        System.out.println("times:"+(end2-begin2));
    }

}
