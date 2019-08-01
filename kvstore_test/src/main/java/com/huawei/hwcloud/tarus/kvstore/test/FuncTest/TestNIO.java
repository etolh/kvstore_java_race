package com.huawei.hwcloud.tarus.kvstore.test.FuncTest;

import com.huawei.hwcloud.tarus.kvstore.race.common.Constant;
import com.huawei.hwcloud.tarus.kvstore.race.common.Utils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

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
    public void testBufferget(){
        ByteBuffer buffer = ByteBuffer.allocate(4);
        byte[] bytes = "ABCD".getBytes();
        System.out.println(bytes.length);
        buffer.get(bytes, 0, -1);
    }

    /**
     * 测试写4个4KB数据后才落盘
     */
    @Test
    public void TestFourBufferWrite() {
        int bufferPosition = 0;
        int times = 102;

        ByteBuffer buffer = ByteBuffer.allocate(4 * Constant.VALUE_LEN);
        for (int i = 1; i <= times; i++) {
            byte[] vals = Utils.buildVal(i);
            buffer.put(vals);
            bufferPosition++;
            if (bufferPosition >= 4){
                buffer.position(0);
                buffer.limit(bufferPosition + Constant.VALUE_LEN);
            }

        }

    }

//    public void write(byte[])
}
