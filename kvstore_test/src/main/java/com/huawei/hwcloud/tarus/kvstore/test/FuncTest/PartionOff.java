package com.huawei.hwcloud.tarus.kvstore.test.FuncTest;

import java.util.concurrent.atomic.AtomicInteger;

public class PartionOff {
    private static AtomicInteger par = new AtomicInteger(0);
    private static AtomicInteger[] off = new AtomicInteger[1024];
    private static final int NUM = 4;

    public void init(){
        for (int i = 0; i < 1024; i++){
            off[i] = new AtomicInteger(0);
        }
    }

    public void run() {
        int parNo = par.get();
        int offNo = off[parNo].getAndIncrement();

        if (offNo >= NUM){
            par.getAndIncrement();
            parNo = par.get();
            offNo = off[parNo].getAndIncrement();
        }
        offNo++;
        System.out.println("thread:"+this+" parNo:"+parNo+" offNo:"+offNo);
    }
}
