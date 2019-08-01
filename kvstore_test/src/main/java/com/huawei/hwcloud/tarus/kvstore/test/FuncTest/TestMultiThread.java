package com.huawei.hwcloud.tarus.kvstore.test.FuncTest;

import com.huawei.hwcloud.tarus.kvstore.race.common.Constant;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 多线程应用操作
 */
public class TestMultiThread {

    /**
     * 测试线程池使用
     */
    @Test
    public void testExecutors() {
        int thread = 8;
        ExecutorService executor = Executors.newFixedThreadPool(thread);
        for (int t = 0; t < 32; t++) {
            // 提交32次任务
            final int index = t;
            executor.execute(new Runnable() {
                @Override
                public void run(){
                    // runnable内部类必须用final
                    System.out.println(Thread.currentThread().getName()+" "+index);
                }
            });
        }
    }

    /**
     * 测试CountDownLatch使用: 将parNum个分区的加载任务分配到threadNum个线程
     */
    @Test
    public void testCountDownLatch() {

        int thread_num = 16;
        CountDownLatch latch = new CountDownLatch(thread_num);
        final int parNum = Constant.PARTITION_NUM;
        for (int i = 1; i <= thread_num; i++) {
            final int index = i;
            // 线程i
            new Thread(new Runnable(){
                @Override
                public void run() {
                    for (int par = 0; par < parNum; par++){
                        // 分区par交给线程index处理
                        if (par % thread_num == index){
                            System.out.println(Thread.currentThread().getName()+" "+par + " : "+index);
                        }
                    }
                    latch.countDown();
                }
            }, "thread"+i).start();
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(Thread.currentThread().getName()+" finish");

        /*
        class myThread extends Thread{
            @Override
            public void run() {
                super.run();
            }
        }

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {

            }
        }, "thread_name");
        t.start();

        // 匿名
        new Thread(new Runnable() {
            @Override
            public void run() {

            }
        }, "thread_name").start();
        */
    }
}

