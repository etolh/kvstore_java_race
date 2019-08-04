package com.huawei.hwcloud.tarus.kvstore.race.common;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class UnsafeUtils {
    public static final Unsafe UNSAFE;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (Unsafe) field.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
