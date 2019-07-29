package com.huawei.hwcloud.tarus.kvstore.test;

import com.huawei.hwcloud.tarus.kvstore.common.KVStoreRace;
import com.huawei.hwcloud.tarus.kvstore.common.Ref;
import com.huawei.hwcloud.tarus.kvstore.race.EngineKVStoreRace;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;

/**
 * 阶段1测试：init - set - write
 */
public class StepOneTest {

    @Test
    public void testWriteRead() {

        int nums = 4000;
        String path = "C:\\Users\\t-liah\\Desktop\\db\\program\\kvstore_java_race\\kvstore_test\\src\\main\\java\\com\\huawei\\hwcloud\\tarus\\kvstore\\test\\data\\data0";
        KVStoreRace race = new EngineKVStoreRace();
        race.init(path, 1);

        // write
        long begin = System.currentTimeMillis();

        for (int i = 1; i <= nums; i++) {
            String key = buildKey(i);
            byte[] val = buildVal(i);
            race.set(key, val);
        }

        for (int  i = nums; i >= 1; i--){
            String key = buildKey(i);
            byte[] val = buildVal(i);
            Ref<byte[]> getVal = Ref.of(byte[].class);
            race.get(key, getVal);
            if (!Arrays.equals(val, getVal.getValue())){
                System.out.println("error");
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("times:"+String.valueOf(end-begin));
        remove_files(path);
    }


    private final String buildKey(final long i) {
        return String.format("%d", i);
    }

    private final byte[] buildVal(final int i) {
        byte[] bytes = new byte[4096];
        Arrays.fill(bytes,(byte)i);
        return bytes;
    }

    private void remove_files(final String dir) {

        File dirFile = new File(dir);

        if(!dirFile.exists()){
            dirFile.mkdirs();
            return;
        }else if(!dirFile.isDirectory()){
            dirFile.delete();
            dirFile.mkdirs();
            return;
        }

        for(File file:dirFile.listFiles()){
            file.delete();
        }
    }
}
