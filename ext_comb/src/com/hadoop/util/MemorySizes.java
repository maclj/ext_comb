package com.hadoop.util;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * refrence:
 * http://www.javaspecialists.co.za/archive/Issue078.html
 * 
 *
 */
public class MemorySizes {
    
    /**
     * 当前使用内存
     * 
     * 由于totalMemory会变化，因此使用该方式。
     * @return
     */
    public static final long usedMemory() {
        // System.gc();
        Runtime run = Runtime.getRuntime();
        return run.totalMemory() - run.freeMemory();
    }
    
    public static final String format(long memory) {
        return String.format("%d M", memory/1024/1024);
    }

    private final Map<Class<?>, Integer> primitiveSizes = new IdentityHashMap<Class<?>, Integer>() {
        private static final long serialVersionUID = -899312646288650807L;

        {
            put(boolean.class, new Integer(1));
            put(byte.class, new Integer(1));
            put(char.class, new Integer(2));
            put(short.class, new Integer(2));
            put(int.class, new Integer(4));
            put(float.class, new Integer(4));
            put(double.class, new Integer(8));
            put(long.class, new Integer(8));
        }
    };

    public int getPrimitiveFieldSize(Class<?> clazz) {
        return ((Integer) primitiveSizes.get(clazz)).intValue();
    }

    public int getPrimitiveArrayElementSize(Class<?> clazz) {
        return getPrimitiveFieldSize(clazz);
    }

    /**
     * 引用，64位 为8,32位为4，这里按照64位处理且不考虑指针压缩。
     * @return
     */
    public int getPointerSize() {
        return 8;
    }

    /**
     * 对象头在32位系统上占用8bytes，64位系统上占用16bytes，这里按照64位处理且不考虑指针压缩。
     * @return
     */
    public int getClassSize() {
        return 16;
    }
    
    /**
     * 数组在32位系统上占用16bytes，64位系统上占用24bytes，这里按照64位处理且不考虑指针压缩。
     * @return
     */
    public int getArraySize() {
        return 24;
    }
}
