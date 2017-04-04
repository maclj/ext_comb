package com.hadoop.entry;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class JobDefineHelper {
    
    /**
     * 是否为缺省的mapper定义。
     * @param jc
     * @return
     */
    public static boolean isDefaultMapperClass(JobDefine jc) {
        if(Mapper.class.equals(jc.mapperClass() )) {
            return true;
        }
        return false;
    }
    
    /**
     * 是否是缺省的Mapper定义
     * @param clazz
     * @return
     */
    @SuppressWarnings("rawtypes")
    public static boolean isDefaultMapperClass(Class<? extends Mapper> clazz) {
        if(Mapper.class.equals(clazz )) {
            return true;
        }
        return false;
    }

    public static boolean isDefaultCombinerClass(JobDefine jc) {
        if(Reducer.class.equals(jc.combinerClass() )) {
            return true;
        }
        return false;
    }
    
    @SuppressWarnings("rawtypes")
    public static boolean isDefaultCombinerClass(Class<? extends Reducer> clazz) {
        if(Reducer.class.equals(clazz )) {
            return true;
        }
        return false;
    }
    
    @SuppressWarnings("rawtypes")
    public static boolean isDefaultReducerClass(Class<? extends Reducer> clazz) {
        if(Reducer.class.equals(clazz )) {
            return true;
        }
        return false;
    }
    
    public static boolean isDefaultPartitionerClass(JobDefine jc) {
        if(Partitioner.class.equals(jc.partitionerClass() )) {
            return true;
        }
        return false;
    }
    
    @SuppressWarnings("rawtypes")
    public static boolean isDefaultPartitionerClass(Class<? extends Partitioner> clazz) {
        if(Partitioner.class.equals(clazz )) {
            return true;
        }
        return false;
    }
    
    public static boolean isDefaultGroupingComparatorClass(JobDefine jc) {
        if(RawComparator.class.equals(jc.groupingComparatorClass() )) {
            return true;
        }
        return false;
    }
    
    @SuppressWarnings("rawtypes")
    public static boolean isDefaultGroupingComparatorClass(Class<? extends RawComparator> clazz) {
        if(RawComparator.class.equals(clazz )) {
            return true;
        }
        return false;
    }
    
    public static String getJobDesc(JobDefine jd) {
        if(jd.jobDesc().trim().length() == 0) {
            return "job." + jd.project() + "."+ jd.jobSeq();
        }
        return jd.jobDesc();
    }
    

}
