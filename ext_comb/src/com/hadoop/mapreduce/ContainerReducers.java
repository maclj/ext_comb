package com.hadoop.mapreduce;

import java.util.List;

import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * container定义。
 * 
 * 
 *
 * @param <KEYOUT>
 * @param <VALUEOUT>
 */
public interface ContainerReducers<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    
    /**
     * 获取当前容器中的Mappers对象。
     * 
     * @return
     */
    @SuppressWarnings("rawtypes")
    List<KeyWrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> createReducers(Context context);
}
