package com.hadoop.mapreduce.lib.input;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 获取Mapper当前读取的路径。
 * 
 * 对于TextInputFormat，只有一个文件，setup0初始化以后都是同一个文件路径。
 * 对于CombineTextInputFormat, map过程会切换至下一个文件，因此每次调用可能返回不同的文件路径。
 * 
 * 注意：
 * 这里未使用单例或者静态变量的原因是，平台支持多个项目并行运行，next调用过程会互相影响。
 * 
 * 
 *
 */
public class MapperPath {

    /** 当前文件路径  */
    private String current = "";
    
    @SuppressWarnings("rawtypes")
    private MapContext context;
    
    /** 是否是合并文件 */
    private boolean isCombine;
    
    @SuppressWarnings("rawtypes")
    public MapperPath(MapContext context) {
        this.context = context;
        InputSplit is = InputsUtils.getSplitOfMultipleInputs(context.getInputSplit());
        if(is instanceof FileSplit) {
            this.current = ((FileSplit)is).getPath().toString();
        } else if(is instanceof CombineFileSplit) {
            this.current = context.getConfiguration().get(MRJobConfig.MAP_INPUT_FILE);
            this.isCombine = true;
        } else {
            throw new IllegalStateException( is.getClass() + " not implemented.");
        }
    }
    
    /**
     * 对应CombineTextInputFormat，需要每次map过程调用next，才能得到正确的当前文件。
     * 
     * @return
     */
    private MapperPath next() {
        if(this.isCombine) {
            this.current = this.context.getConfiguration().get(MRJobConfig.MAP_INPUT_FILE);
        }
        return this;
    }
    
    /**
     * 获取当前文件的标准路径
     * @return
     */
    public String getCanonicalPath() {
        next();
        return this.current;
    }
    
    /**
     * 获取当前文件名称
     * @return
     */
    public String getName() {
        next();
        int start = this.current.lastIndexOf("/");
        if(start == this.current.length() -1) {
            return "";
        }
        return this.current.substring(start);
    }
    
}
