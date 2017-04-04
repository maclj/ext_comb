package com.hadoop.mapreduce;

/**
 * 
 * 输入数据包含区分标识，输出数据包含区分标识。
 * 
 * 
 *
 */
public abstract class KeyInOutReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        extends KeyWrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    @Override
    protected boolean isKeyDataIn() {
        return true;
    }

    @Override
    protected boolean isKeyDataOut() {
        return true;
    }

}
