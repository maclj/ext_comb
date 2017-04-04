package com.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

/**
 * 容器中适配旧版本实现的adapter。
 * 
 * 不按照新流程实现，但是需要被容器管理的类，需要实现。
 * 
 * 
 *
 * @param <KEYIN>
 * @param <VALUEIN>
 * @param <KEYOUT>
 * @param <VALUEOUT>
 */
public class FitKeyMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends KeyWrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    /**
     * 适配旧版本，不走拆分key的流程。
     * @return
     */
    protected boolean isFitToOld() {
        return true;
    }

    @Override
    public int getOutputSplitKey() {
        return 0;
    }

    @Override
    public void map0(String value, Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context)
            throws IOException, InterruptedException {
        //Nothing
    }

    @Override
    protected boolean isKeyDataIn() {
        return false;
    }

    @Override
    protected boolean isKeyDataOut() {
        return false;
    }

}
