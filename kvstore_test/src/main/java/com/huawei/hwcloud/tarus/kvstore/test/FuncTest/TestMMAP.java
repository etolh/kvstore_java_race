package com.huawei.hwcloud.tarus.kvstore.test.FuncTest;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

/**
 * @ClassName TestMMAP
 * @Description TODO
 * @Author lianghu
 * @Date 2019-07-26 23:30
 * @VERSION 1.0
 */
public class TestMMAP {

    public void testMMAPRead(String file) {

        try {
            RandomAccessFile accessFile = new RandomAccessFile(new File(file), "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }
}
