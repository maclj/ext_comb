package com.hadoop.mapreduce;

/**
 * 输入数据包含区分标识，输出数据不包含区分标识。
 * 
 *
 * @param <KEYOUT>
 * @param <VALUEOUT>
 */
public abstract class KeyInMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends KeyWrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    @Override
    protected boolean isKeyDataIn() {
        return true;
    }

    @Override
    protected boolean isKeyDataOut() {
        return false;
    }

}
