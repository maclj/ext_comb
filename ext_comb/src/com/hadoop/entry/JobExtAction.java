package com.hadoop.entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * 任务扩展动作。
 * 
 *
 */
public interface JobExtAction {
    
    /**
     * 肯定会被调用的实现，用于特殊实现。
     * 
     * @param lastJob 上一个任务对象，可能为null。
     * @param conf
     * @param jc
     * @param args
     */
    void executeAlways(Job lastJob, Configuration conf, JobDefine jc, String[] args);
    
    
    /**
     * 在Job构建之前调用的回调动作，如设置分隔符号（该动作必须在job创建之前调用）。
     * @param lastJob 上一个任务对象，可能为null。
     * @param conf
     * @param jc
     * @param args
     */
    void executeFirst(Job lastJob, Configuration conf, JobDefine jc, String[] args);
    
    /**
     * 在Job构建之后最后调用的回调动作，缺省实现为空。
     * @param currentjob 当前任务对象
     * @param conf
     * @param jc
     * @param args
     */
    void executeLast(Job currentjob, Configuration conf, JobDefine jc, String[] args);
    
    /**
     * 此Job 是否需要忽略运行。
     * 部分情况下，Job是根据参数等情况来决定是否需要存在的，
     * 缺省为false,true的情况需要业务定义。
     * @param conf
     * @param jc
     * @param args
     * @return
     */
    boolean isJobIgnored(Configuration conf, JobDefine jc, String[] args);
    
    
    /**
     * 自定义任务名称
     * @param conf
     * @param jc
     * @param args
     * @return
     */
    String buildJobDesc(Configuration conf, JobDefine jc, String[] args);
}
