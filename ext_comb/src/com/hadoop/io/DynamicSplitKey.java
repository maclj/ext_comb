package com.hadoop.io;

/**
 * 实现此接口，提供增加splitkey的入口。
 * 
 *
 */
public interface DynamicSplitKey extends SplitValue {
    
    DynamicSplitKey append(int append, String separator);

}
