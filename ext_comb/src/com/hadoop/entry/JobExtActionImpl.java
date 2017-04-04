package com.hadoop.entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * 缺省的任务扩展动作实现
 * 
 *
 */
public class JobExtActionImpl implements JobExtAction {

    @Override
    public void executeAlways(Job lastJob, Configuration conf, JobDefine jc, String[] args) {
        // DO NOTHING
    }
    
    /**
     * 缺省设置分隔符号为"|";
     */
    @Override
    public void executeFirst(Job lastJob, Configuration conf, JobDefine jc, String[] args) {
        
        // 获取job输出时的分隔符号。
        String separator = jc.outputMapreduceSeparator();
        // see TextOutputFormat.SEPERATOR
        if(separator.length() != 0) {
            conf.set("mapreduce.output.textoutputformat.separator", separator);
        }
    }

    @Override
    public void executeLast(Job job, Configuration conf, JobDefine jc, String[] args) {
        // donothing 
    }

    /**
     * 缺省实现为不需要忽略当前任务。
     */
    @Override
    public boolean isJobIgnored(Configuration conf, JobDefine jc, String[] args) {
        return false;
    }

    
    @Override
    public String buildJobDesc(Configuration conf, JobDefine jc, String[] args) {
        return null;
    }

}
