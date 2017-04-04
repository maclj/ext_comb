package com.hadoop.mapreduce;

import java.util.List;

import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * container定义。
 * 
 * 
 *
 * @param <KEYOUT>
 * @param <VALUEOUT>
 */
public interface ContainerMappers<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    /**
     * 获取当前容器中的Mappers对象。
     * 
     * @return
     */
    @SuppressWarnings("rawtypes")
    List<KeyWrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> createMappers(Context context);
}
