package com.huawei.hwcloud.tarus.kvstore.test.FuncTest;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

/**
 * @ClassName TestMMAP
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
