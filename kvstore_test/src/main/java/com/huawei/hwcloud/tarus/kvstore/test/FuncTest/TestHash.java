package com.huawei.hwcloud.tarus.kvstore.test.FuncTest;

import com.huawei.hwcloud.tarus.kvstore.race.Utils;
import org.junit.jupiter.api.Test;

/**
 * @ClassName TestHash
 * @Description TODO
 * @Author lianghu
 * @Date 2019-07-28 17:07
 * @VERSION 1.0
 */
public class TestHash {

    @Test
    public void testHash(){

        for (int i = 10000900; i <= 10001000; i++){
            String key = buildKey(i);
            long numKey = Long.parseLong(key);
            System.out.println(numKey);
//            System.out.println("hash1:"+Utils.valueFileHash(numKey));
            System.out.println("hash2:"+Utils.valueFileHash2(numKey));

        }
    }

    private final String buildKey(final long i) {
        return String.format("%d", i);
    }

}
