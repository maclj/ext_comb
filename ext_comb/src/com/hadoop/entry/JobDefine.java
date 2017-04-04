package com.hadoop.entry;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

@Retention(RetentionPolicy.RUNTIME)
@Target({java.lang.annotation.ElementType.TYPE})
public @interface JobDefine {
    
    /**
     * 项目名称，缺省为空。
     * @return
     */
    String project() default "";

    /**
     * 任务顺序，必须从0开始计数。
     * @return
     */
    int jobSeq() default 0;
    
    /**
     * 是否同步运行，多个job组成的顺序依赖任务中，不能设置成false（由业务保证）
     * @return
     */
    boolean sync() default true;
    
    /**
     * 自定义的任务名称，为空则自动生成。
     * 形如："job." + jd.project() + "."+ jd.jobSeq();
     * 
     * @return
     */
    String jobDesc() default "";
    
    /**
     * 输入路径，缺省为空。
     * @return
     */
    String inputPath() default "";
    
    /**
     * 输出路径，缺省为空。
     * @return
     */
    String outputPath() default "";
    
    /**
     * 主类信息，缺省为统一入口。
     * @return
     */
    Class<?> jarByClass() default Main.class;
    
    /**
     * mapper
     * @return
     */
    @SuppressWarnings("rawtypes")
    Class<? extends Mapper> mapperClass() default Mapper.class;
    
    /**
     * combiner
     * @return
     */
    @SuppressWarnings("rawtypes")
    Class<? extends Reducer> combinerClass() default Reducer.class;
    
    /**
     * reducer
     * @return
     */
    @SuppressWarnings("rawtypes")
    Class<? extends Reducer> reducerClass() default Reducer.class;
    
    /**
     * partitioner
     * @return
     */
    @SuppressWarnings("rawtypes")
    Class<? extends Partitioner> partitionerClass() default Partitioner.class;
    
    /**
     * groupingComparator
     * @return
     */
    @SuppressWarnings("rawtypes")
    Class<? extends RawComparator> groupingComparatorClass() default RawComparator.class;
    
    // CombinerKeyGroupingComparatorClass
    // SortComparatorClass
    
    /**
     * mapOutputKey
     * @return
     */
    Class<?> mapOutputKeyClass() default Text.class;
    
    /**
     * mapOutputValue
     * @return
     */
    Class<?> mapOutputValueClass() default Text.class;
    
    /**
     * outputKey
     * @return
     */
    Class<?> outputKeyClass() default Text.class;
    
    /**
     * outputValue
     * @return
     */
    Class<?> outputValueClass() default Text.class;
    
    /**
     * numReduceTasks
     * @return
     */
    int numReduceTasks() default 0;
    
    /**
     * 是否延迟输出
     */
    boolean lazyOutputEnabled() default false;
    
    /**
     * inputFormat
     * @return
     */
    @SuppressWarnings("rawtypes")
    Class<? extends InputFormat> inputFormatClass() default CombineTextInputFormat.class;
    
    /**
     * outputFormat
     * @return
     */
    @SuppressWarnings("rawtypes")
    Class<? extends OutputFormat> outputFormatClass() default TextOutputFormat.class;
    
    /**
     * inputDirRecursive
     * @return
     */
    boolean inputDirRecursive() default true;
    
    /**
     * 是否压缩输出
     * @return
     */
    boolean compressOutput() default true;
    
    /**
     * 输出压缩实现类
     * @return
     */
    Class<? extends CompressionCodec> outputCompressorClass() default GzipCodec.class;
    
    /**
     * 任务的区分标识，在数据中为第一个输出分隔符前面的内容，缺省为空。
     * 
     * 合并任务中，需要找到对应容器管理的mapper或者reducer所使用的splitKey（正常一个job应该是一样的）
     * 并通过annotation或者conf建立对应关系，过程很麻烦，且该值应该在项目开发阶段完成，
     * 因此，目前忽略，实现简化版本，在keymapper和keyreducer中自行实现。
     * @return
     */
    // String outputSplitKey() default "";
    
    /**
     * 文本输出时的分隔符,缺省为|
     * @return
     */
    // 使用-Denv.output.separator="|"指定，缺省为"|"
    // String outputSeparator() default "|";
    
    /**
     * reduce输出时候的分隔符（区分key和value的），为空时不设置；
     * 缺省为|
     * 
     * @return
     */
    String outputMapreduceSeparator() default "|";
    
    /**
     * 是否启用计数器
     * @return
     */
    boolean countersEnabled() default true;
    
    /**
     * 设置任务的输入输出路径，缺省实现为在输入参数的输入输出路径中补充inputpath和outputPath信息。
     * 
     * @return
     */
    Class<? extends JobPathResolver> jobPathResolver() default JobPathResolverImpl.class;
    
    /**
     * 任务设置的扩展动作，在任务配置流程结束后回调，由业务指定对应的扩展方法。
     * 缺省实现为设置分隔符号为"|"。
     * 
     * @return
     */
    Class<? extends JobExtAction> extAction() default JobExtActionImpl.class;
    
    /**
     * 容器管理的mapper
     * @return
     */
    Class<?>[] containerMappers() default Object.class;
    
    /**
     * 容器管理的combiner
     * @return
     */
    Class<?>[] containerCombiners() default Object.class;
    
    /**
     * 容器管理的reducer
     * @return
     */
    Class<?>[] containerReducers() default Object.class;
}
