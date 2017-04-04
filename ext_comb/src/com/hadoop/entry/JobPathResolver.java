package com.hadoop.entry;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public interface JobPathResolver {

    void input(Job job, Configuration conf, JobDefine jc, String[] args) throws IllegalArgumentException, IOException;
    
    void output(Job job, Configuration conf, JobDefine jc, String[] args);
}
