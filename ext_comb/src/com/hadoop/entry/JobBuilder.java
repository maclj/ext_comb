package com.hadoop.entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.hadoop.io.SplitOutputKey;
import com.hadoop.mapreduce.ContainerKeyCombinerImpl;
import com.hadoop.mapreduce.ContainerKeyCombinerTextImpl;
import com.hadoop.mapreduce.ContainerKeyMapperImpl;
import com.hadoop.mapreduce.ContainerKeyReducerImpl;
import com.hadoop.mapreduce.ContainerResolver;
import com.hadoop.plat.util.SplitValueBuilder;

/**
 * 根据JobDefine定义构建单个项目中的所有job。
 * 
 * 
 *
 */
public class JobBuilder {
    
    /** 日志对象  */
    private static Log LOG = LogFactory.getLog(JobBuilder.class);

    /**
     * hadoop configuration
     */
    private Configuration conf;

    /** 任务定义 */
    private JobDefine jobDefine;
    
    /** 任务参数解析器  */
    private JobDefineResolver jdr;

    /** 程序自定义命令行参数 */
    private String[] args;
    
    /** 任务扩展动作实现 */
    private JobExtAction jea;
    
    /** 上一个任务引用 */
    private Job lastJob;
    
    /** 是否初始化成功 */
    private boolean status;

    /**
     * 构造函数
     * @param conf
     * @param jc
     * @param args
     * @param jdr
     */
    public JobBuilder(Configuration conf, JobDefine jc, String[] args, JobDefineResolver jdr, Job lastJob) {
        this.conf = conf;
        this.jobDefine = jc;
        this.args = args;
        this.jdr = jdr;
        this.lastJob = lastJob;
        
        try {
            this.jea = this.jobDefine.extAction().newInstance();
        } catch (Exception e) {
            LOG.warn(null, e);
            return;
        } 
        // 初始化就绪
        this.status = true;
    }

    /**
     * 构建Job。
     * @return
     */
    @SuppressWarnings("rawtypes")
    public Job build() {
        try {
            
            // 固定回调，用于特殊分支
            this.jea.executeAlways(this.lastJob, this.conf, this.jobDefine, this.args);
            
            // 调用扩展动作，设置必须在Job构建之前赋值的参数。
            // JobExtAction jea = this.jc.extAction().newInstance();
            boolean ignoreJob = this.jea.isJobIgnored(conf, jobDefine, args);
            if(ignoreJob) {
                return null;
            }
            // 调用扩展动作
            jea.executeFirst(this.lastJob, this.conf, jobDefine, args);
            // 追加conf配置
            this.addConf();
            
            // 注意需要传递给各node的配置，需要在job创建以前赋值。
            String jobName = getJobDesc();
            Job job = Job.getInstance(this.conf, jobName);
            
            job.setJarByClass(this.jobDefine.jarByClass());
            job.setInputFormatClass(jdr.parseInputFormat(this.jobDefine.inputFormatClass()));
            
            Class<? extends Mapper> mapperClass = jdr.parseMapper(this.jobDefine.mapperClass());
            if(!JobDefineHelper.isDefaultMapperClass(mapperClass)) {
                job.setMapperClass(this.jobDefine.mapperClass());
            } else {
                // 如果mapper是缺省值，且设置容器管理类，则赋缺省值
                if(this.conf.get(Jobs.JOB_COMB_MAPPERS, null) != null) {
                    job.setMapperClass(ContainerKeyMapperImpl.class);
                }
            }
            Class<?> mapOutputKeyClass = this.jdr.parseClass("mapOutputKeyClass", this.jobDefine.mapOutputKeyClass());
            job.setMapOutputKeyClass(mapOutputKeyClass);
            job.setMapOutputValueClass(this.jdr.parseClass("mapOutputValueClass", this.jobDefine.mapOutputValueClass()));
            
            int numReduceTasks = jdr.parse("numReduceTasks", this.jobDefine.numReduceTasks());
            job.setNumReduceTasks(numReduceTasks);

            // 设置reduce
            if (numReduceTasks > 0) {

                if (!JobDefineHelper.isDefaultCombinerClass(jobDefine)) {
                    job.setCombinerClass(this.jobDefine.combinerClass());
                } else {
                    // 如果combiner是缺省值，且设置容器管理类，则赋缺省值
                    if(this.conf.get(Jobs.JOB_COMB_COMBINERS, null) != null) {
                        // 为预置的两个实现赋初始值
                        if(mapOutputKeyClass == Text.class) {
                            job.setCombinerClass(ContainerKeyCombinerTextImpl.class);
                        } else if(mapOutputKeyClass == SplitOutputKey.class) {
                            job.setCombinerClass(ContainerKeyCombinerImpl.class);
                        }
                    }
                }
                if (!JobDefineHelper.isDefaultPartitionerClass(jobDefine)) {
                    job.setPartitionerClass(this.jobDefine.partitionerClass());
                }
                if (!JobDefineHelper.isDefaultGroupingComparatorClass(jobDefine)) {
                    job.setGroupingComparatorClass(this.jobDefine.groupingComparatorClass());
                }
                Class<? extends Reducer> reducer = jdr.parseReducer(this.jobDefine.reducerClass());
                if(!JobDefineHelper.isDefaultReducerClass(reducer)) {
                    job.setReducerClass(reducer);
                } else {
                    // 如果reducer是缺省值，且设置容器管理类，则赋缺省值
                    if(this.conf.get(Jobs.JOB_COMB_REDUCERS, null) != null) {
                        job.setReducerClass(ContainerKeyReducerImpl.class);
                    }
                }
                
                job.setOutputKeyClass(this.jdr.parseClass("outputKeyClass", this.jobDefine.outputKeyClass()));
                job.setOutputValueClass(this.jdr.parseClass("outputValueClass", this.jobDefine.outputValueClass()));
            }
            
            // 调用路径设置
            JobPathResolver jpr = this.jobDefine.jobPathResolver().newInstance();
            jpr.input(job, this.conf, this.jobDefine, this.args);
            // 如果设置了递归目录
            if (this.jobDefine.inputDirRecursive()) {
                FileInputFormat.setInputDirRecursive(job, true);
            }
            jpr.output(job, this.conf, this.jobDefine, this.args);
            
            // 调用其他追加设置
            boolean lazyOutputEnabled = jdr.parse("lazyOutputEnabled", this.jobDefine.lazyOutputEnabled());
            if (lazyOutputEnabled) {
                LazyOutputFormat.setOutputFormatClass(job, this.jobDefine.outputFormatClass());// 不要默认输出
            }
            boolean compressOutput = jdr.parse("compressOutput", this.jobDefine.compressOutput());
            if (compressOutput) {
                FileOutputFormat.setCompressOutput(job, true);
                Class<? extends CompressionCodec> compressionCodec = jdr
                        .parseOutputCompressorClass(this.jobDefine.outputCompressorClass());
                FileOutputFormat.setOutputCompressorClass(job, compressionCodec);
            }
            boolean countersEnabled = jdr.parse("countersEnabled", this.jobDefine.countersEnabled());
            if (countersEnabled) {
                MultipleOutputs.setCountersEnabled(job, true);
            }
            
            // 调用扩展动作
            jea.executeLast(job, this.conf, this.jobDefine, this.args);
            
            return job;
        } catch (Exception e) {
            LOG.warn(null, e);
        }
        return null;
    }
    
    /**
     * 获取任务描述，任务名称的顺序为：
     * 1、获取自定义实现中返回的任务描述；
     * 2、获取Annotation中JobDesc中的定义；
     * 3、返回"job." + jd.project() + "."+ jd.jobSeq() + date;
     * @return
     */
    public String getJobDesc() {
        String jobDesc = this.jea.buildJobDesc(this.conf, this.jobDefine, this.args);
        if(jobDesc != null && jobDesc.trim().length() != 0) {
            return jobDesc;
        }
        return JobDefineHelper.getJobDesc(this.jobDefine)
                + (Jobs.getRunDate(this.conf).length() == 0 ? "" : "_" + Jobs.getRunDate(this.conf))
                ;
    }
    
    /**
     * 初始化是否就绪。
     * @return
     */
    public boolean isOk() {
        return this.status;
    }
    
    /**
     * 执行conf的追加配置(必须在构造Job之前的设置)
     */
    private void addConf() {
        // 设置当前任务标识
        this.conf.set(Jobs.JOB_CURRENT, 
                String.format("%s.%d", this.jobDefine.project(), this.jobDefine.jobSeq()));
        
        String containerClasses = null;
        Class<?>[] clazzArray = null;
        SplitValueBuilder clazzDesc = new SplitValueBuilder(",");
        
        // 优先走命令行参数
        containerClasses = this.jdr.parseContainerClasses("containerMappers", null);
        if (containerClasses != null) {
            this.conf.set(Jobs.JOB_COMB_MAPPERS, containerClasses);
        } else {
            clazzArray = this.jobDefine.containerMappers();
            if (!ContainerResolver.isUndefined(clazzArray)) {
                clazzDesc.clear();
                for (Class<?> clazz : clazzArray) {
                    clazzDesc.add(clazz.getCanonicalName());
                }
                this.conf.set(Jobs.JOB_COMB_MAPPERS, clazzDesc.build());
            }
        }
        
        containerClasses = this.jdr.parseContainerClasses("containerCombiners", null);
        if (containerClasses != null) {
            this.conf.set(Jobs.JOB_COMB_COMBINERS, containerClasses);
        } else {
            clazzArray = this.jobDefine.containerCombiners();
            if (!ContainerResolver.isUndefined(clazzArray)) {
                clazzDesc.clear();
                for (Class<?> clazz : clazzArray) {
                    clazzDesc.add(clazz.getCanonicalName());
                }
                this.conf.set(Jobs.JOB_COMB_COMBINERS, clazzDesc.build());
            }
        }
        
        containerClasses = this.jdr.parseContainerClasses("containerReducers", null);
        if (containerClasses != null) {
            this.conf.set(Jobs.JOB_COMB_REDUCERS, containerClasses);
        } else {
            clazzArray = this.jobDefine.containerReducers();
            if (!ContainerResolver.isUndefined(clazzArray)) {
                clazzDesc.clear();
                for (Class<?> clazz : clazzArray) {
                    clazzDesc.add(clazz.getCanonicalName());
                }
                this.conf.set(Jobs.JOB_COMB_REDUCERS, clazzDesc.build());
            }
        }
    }
    

}
