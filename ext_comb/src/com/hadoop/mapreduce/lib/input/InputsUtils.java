package com.hadoop.mapreduce.lib.input;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.mapreduce.InputSplit;

/**
 * 获取InputSplit对象，以获取对应的文件信息
 *
 */
public class InputsUtils {
    private static final String TAGGED_INPUT_SPLIT_CLASS_NAME =
            "org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit";
    private static Class<?> TAGGED_INPUT_SPLIT_CLASS;
    private static Method GET_INPUT_SPLIT_METHOD;
    static {
        Class<?> clazz = null;
        try {
            clazz = Class.forName(TAGGED_INPUT_SPLIT_CLASS_NAME, false, InputsUtils.class.getClassLoader());
            if (clazz != null) {
                TAGGED_INPUT_SPLIT_CLASS = clazz;
                initMethod();
            }
        } catch (Exception e) {
            e.printStackTrace();
            // ignored;
        }
        
    }

    private static void initMethod() throws NoSuchMethodException, SecurityException {
        if (TAGGED_INPUT_SPLIT_CLASS == null)
            return;
        Method method;
        method = TAGGED_INPUT_SPLIT_CLASS.getDeclaredMethod("getInputSplit");
        if (method != null) {
            GET_INPUT_SPLIT_METHOD = method;
            method.setAccessible(true);
        }
    }

    /**
     * 使用 MultipleInputs 的程序，Mapper.Context.getInputSplit()返回的是TaggedInputSplit，该方法返回当前的TaggedInputSplit.split。
     * @return
     * @since 1.0
     */
    public static InputSplit getSplitOfMultipleInputs(InputSplit split) {
        try {
            return isMultipleInputSplit(split) ? getInputSplit0(split) : split;
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Reflection Operation Failed!", e);
        }
    }

    private static InputSplit getInputSplit0(InputSplit split)
        throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        if (TAGGED_INPUT_SPLIT_CLASS == null && GET_INPUT_SPLIT_METHOD == null) {
            return split;
        }
        if (GET_INPUT_SPLIT_METHOD != null) {
            return (InputSplit) GET_INPUT_SPLIT_METHOD.invoke(split);
        }
        return split;
    }

    private static boolean isMultipleInputSplit(InputSplit split) {
        if (TAGGED_INPUT_SPLIT_CLASS != null && split.getClass() == TAGGED_INPUT_SPLIT_CLASS) {
            return true;
        } else if (TAGGED_INPUT_SPLIT_CLASS == null) {
            Class<?> clazz = split.getClass();
            if (TAGGED_INPUT_SPLIT_CLASS_NAME.equals(clazz.getName())) {
                TAGGED_INPUT_SPLIT_CLASS = clazz;
                return true;
            }
            return false;
        } else {
            return false;
        }
    }
}
