package com.huawei.hwcloud.tarus.kvstore.test.FuncTest;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestMultiThread {

    @Test
    public void testExe() {
        int thread = 8;
        int times = 8;
        ExecutorService executor = Executors.newFixedThreadPool(thread);
        for (int t = 0; t < thread; t++) {
            PartionOff partionOff = new PartionOff();
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    partionOff.init();
                    for (int i = 0; i < times; i++)
                        partionOff.run();
                }
            });
        }
    }
}
