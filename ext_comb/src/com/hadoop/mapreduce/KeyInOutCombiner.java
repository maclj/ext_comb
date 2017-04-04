package com.hadoop.mapreduce;

/**
 * 
 * Combiner 缺省实现，用于辅助说明，
 * 
 * 如果map输出包含key，Combiner应该输入和输出都配置成包含key，以便与后续的reducer进行兼容。
 * 
 * 
 *
 */
public abstract class KeyInOutCombiner<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
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
