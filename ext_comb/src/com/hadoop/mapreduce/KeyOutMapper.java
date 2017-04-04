package com.hadoop.mapreduce;

/**
 * 输入数据不包含区分标识，输出数据包含区分标识。
 * 
 *
 * @param <KEYOUT>
 * @param <VALUEOUT>
 */
public abstract class KeyOutMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends KeyWrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    
    @Override
    protected boolean isKeyDataIn() {
        return false;
    }

    @Override
    protected boolean isKeyDataOut() {
        return true;
    }
}
