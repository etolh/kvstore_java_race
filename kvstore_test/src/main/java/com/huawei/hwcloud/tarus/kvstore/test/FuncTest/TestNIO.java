package com.huawei.hwcloud.tarus.kvstore.test.FuncTest;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
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

}
