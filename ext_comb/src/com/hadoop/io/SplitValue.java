package com.hadoop.io;

/**
 * 拥有拆分标识的序列化对象实际返回的业务值。
 * 
 * 
 *
 */
public interface SplitValue {

    /**
     * 返回实际的业务数据
     * @return
     */
    String toReal();
}
