package com.hadoop.entry;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 根据Job定义和 Configuration，解析当前Job所需要的运行期参数。
 * 
 * 样例：
 * -Djob.jobName.0.numReduceTasks=24
 * 设置第一个job的numReduceTasks为24.
 * 
 * 注意：
 * 1、JobDefineResolver 与 JobDefine 为一一对应的关系，只存储当前JobDefine相关的参数名和参数值。
 * 2、所有运行期参数与JobDefine中的方法名称相同;
 * 3、job.%d.param为固定前缀，其中%d为job的索引。
 * 
 * 
 *
 */
public class JobDefineResolver {
    
    /** 运行参数的前缀,%s:jobName, %d:jobSeq */
    private static final String JOB_PARAM_PREFIX = "job.%s.%d.";

    /** 任务定义  */
    private JobDefine jd;
    
    /** 上下文 */
    private Configuration conf;
    
    /** 存放运行期参数键值对，注意：未携带前缀信息 且只包含存在定义的参数配置 */
    private Map<String, String> params;
    
    /**
     * 
     * @param conf
     * @param jd
     */
    public JobDefineResolver(Configuration conf, JobDefine jd) {
        
        this.conf = conf;
        this.jd = jd;
        this.params = new HashMap<String, String>();
        String prefix = String.format(JOB_PARAM_PREFIX, this.jd.project(), this.jd.jobSeq());
        
        Method[] methods = this.jd.getClass().getDeclaredMethods();
        String paramName = null;
        String paramValue = null;
        for(Method m : methods) {
            paramName = prefix + m.getName();
            paramValue = this.conf.get(paramName, null);
            if(paramValue == null) {
                continue; // 运行期间无此定义
            }
            this.params.put(m.getName(), paramValue);
        }
    }
    
    /**
     * 
     * @param paramName
     * @param defaultValue
     * @return
     */
    public boolean parse(String paramName, boolean defaultValue) {
        String paramValue = this.params.get(paramName);
        if(paramValue == null) {
            return defaultValue;
        }
        return Boolean.valueOf(paramValue);
    }
    
    /**
     * 
     * @param paramName
     * @param defaultValue
     * @return
     */
    public String parse(String paramName, String defaultValue) {
        String paramValue = this.params.get(paramName);
        if(paramValue == null) {
            return defaultValue;
        }
        return paramValue;
    }
    
    /**
     * 获取运行参数中的整形配置定义。
     * 
     * @param paramName
     * @param defaultValue
     * @return
     */
    public int parse(String paramName, int defaultValue) {
        String paramValue = this.params.get(paramName);
        if(paramValue == null) {
            return defaultValue;
        }
        int value = defaultValue;
        try {
            value = Integer.parseInt(paramValue);
        } catch(NumberFormatException e) {
            e.printStackTrace();
        }
        return value;
    }
    
    /**
     * 获取运行参数中的InputFormat定义，必须为全限定名称。
     * 未配置或者加载失败的情况下，使用缺省值。
     * 
     * @param defaultValue
     * @return
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Class<? extends InputFormat> parseInputFormat(Class<? extends InputFormat> defaultValue) {
        String paramName = "inputFormatClass";
        String paramValue = this.params.get(paramName);
        if(paramValue == null) {
            return defaultValue;
        }
        Class<? extends InputFormat> clazz = null;
        try {
            clazz = (Class<? extends InputFormat>) ClassFinder.getDefaultClassLoader().loadClass(paramValue);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return defaultValue;
        }
        return clazz;
    }
    
    /**
     * 获取运行参数中的Mapper定义，必须为全限定名称。
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Class<? extends Mapper> parseMapper(Class<? extends Mapper> defaultValue) {
        String paramName = "mapperClass";
        String paramValue = this.params.get(paramName);
        if(paramValue == null) {
            return defaultValue;
        }
        Class<? extends Mapper> clazz = null;
        try {
            clazz = (Class<? extends Mapper>) ClassFinder.getDefaultClassLoader().loadClass(paramValue);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return defaultValue;
        }
        return clazz;
    }
    
    /**
     * 获取运行参数中的Reducer定义，必须为全限定名称。
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Class<? extends Reducer> parseReducer(Class<? extends Reducer> defaultValue) {
        String paramName = "reducerClass";
        String paramValue = this.params.get(paramName);
        if(paramValue == null) {
            return defaultValue;
        }
        Class<? extends Reducer> clazz = null;
        try {
            clazz = (Class<? extends Reducer>) ClassFinder.getDefaultClassLoader().loadClass(paramValue);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return defaultValue;
        }
        return clazz;
    }
    
    /**
     * 获取运行参数中的Reducer定义，必须为全限定名称。
     */
    @SuppressWarnings({ "unchecked" })
    public Class<? extends CompressionCodec> parseOutputCompressorClass(Class<? extends CompressionCodec> defaultValue) {
        String paramName = "outputCompressorClass";
        String paramValue = this.params.get(paramName);
        if(paramValue == null) {
            return defaultValue;
        }
        Class<? extends CompressionCodec> clazz = null;
        try {
            clazz = (Class<? extends CompressionCodec>) ClassFinder.getDefaultClassLoader().loadClass(paramValue);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return defaultValue;
        }
        return clazz;
    }
    
    /**
     * 获取运行参数中的ContainerMappers\ContainerReducers\ContainerCombiners定义，必须为全限定名称。
     */
    public String parseContainerClasses(String paramName, String defaultValue) {
        String paramValue = this.params.get(paramName);
        if(paramValue == null) {
            return defaultValue;
        }
        return paramValue;
    }
    
    /**
     * 获取运行参数中的通用Class定义，必须使用全限定名称
     * 
     * 如mapOutputKeyClass 等等。
     * 
     */
    public Class<?> parseClass(String paramName, Class<?> defaultValue) {
        String paramValue = this.params.get(paramName);
        if(paramValue == null) {
            return defaultValue;
        }
        Class<?> value = ClassFinder.loadClass(paramValue);
        return value;
    } 
    
}
