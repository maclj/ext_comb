package com.hadoop.io;

/**
 * 数据区分标识
 * 
 *
 */
public interface SplitKey extends SplitValue {

    int getSplitKey();
    
    void setSplitKey(int key);
}
