package com.hadoop.util;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Stack;

/**
 * refrence:
 * http://www.javaspecialists.co.za/archive/Issue078.html
 * 
 *
 */
public final class MemoryCounter {
    
    private static final MemorySizes sizes = new MemorySizes();
    private final Map<Object, Object> visited = new IdentityHashMap<Object, Object>();
    private final Stack<Object> stack = new Stack<Object>();

    public synchronized long estimate(Object obj) {

        long result = _estimate(obj);
        while (!stack.isEmpty()) {
            result += _estimate(stack.pop());
        }
        visited.clear();
        return result;
    }

    private boolean skipObject(Object obj) {
        if (obj instanceof String) {
            // this will not cause a memory leak since
            // unused interned Strings will be thrown away
            if (obj == ((String) obj).intern()) {
                return true;
            }
        }
        return (obj == null) || visited.containsKey(obj);
    }

    private long _estimate(Object obj) {
        if (skipObject(obj)) {
            return 0;
        }
        
        visited.put(obj, null);
        long result = 0;
        Class<?> clazz = obj.getClass();
        if (clazz.isArray()) {
            return _estimateArray(obj);
        }
        while (clazz != null) {
            Field[] fields = clazz.getDeclaredFields();
            for (int i = 0; i < fields.length; i++) {
                if (!Modifier.isStatic(fields[i].getModifiers())) {
                    if (fields[i].getType().isPrimitive()) {
                        result += sizes.getPrimitiveFieldSize(fields[i].getType());
                    } else {
                        result += sizes.getPointerSize();
                        fields[i].setAccessible(true);
                        try {
                            Object toBeDone = fields[i].get(obj);
                            if (toBeDone != null) {
                                stack.add(toBeDone);
                            }
                        } catch (IllegalAccessException ex) {
                            // assert false;
                        }
                    }
                }
            }
            clazz = clazz.getSuperclass();
        }
        result += sizes.getClassSize();
        return roundUpToNearestEightBytes(result);
    }

    private long roundUpToNearestEightBytes(long result) {
        if ((result % 8) != 0) {
            result += 8 - (result % 8);
        }
        return result;
    }

    protected long _estimateArray(Object obj) {
        long result = sizes.getArraySize();
        int length = Array.getLength(obj);
        if (length != 0) {
            Class<?> arrayElementClazz = obj.getClass().getComponentType();
            if (arrayElementClazz.isPrimitive()) {
                int tmpSize = sizes.getPrimitiveArrayElementSize(arrayElementClazz);
                result += length * tmpSize;
            } else {
                for (int i = 0; i < length; i++) {
                    result += sizes.getPointerSize() + _estimate(Array.get(obj, i));
                }
            }
        }
        return result;
    }
}
