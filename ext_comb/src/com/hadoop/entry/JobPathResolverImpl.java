package com.hadoop.entry;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hadoop.util.Logger;

/**
 * 简单实现，处理输入输出路径。
 * 
 *
 */
public class JobPathResolverImpl implements JobPathResolver {

    @Override
    public void input(Job job, Configuration conf, JobDefine jc, String[] args) throws IllegalArgumentException, IOException {
        String input = Jobs.getInput(conf) + jc.inputPath();
        print("Job Input: " + input);
        FileInputFormat.addInputPath(job, new Path(input));
    }

    @Override
    public void output(Job job, Configuration conf, JobDefine jc, String[] args) {
      String output = Jobs.getOutputFinal(conf) + jc.outputPath();
      String prefix = Jobs.getOutputPrefix(conf);
      if(prefix.length() != 0) {
          output = prefix + jc.outputPath();
      }
      print("Job Output: " + output);
      FileOutputFormat.setOutputPath(job, new Path(output));
    }
    
    /**
     * 打印输出
     * @param conf
     * @param value
     */
    protected void print(String value) {
        Logger.print(value);
    }

}
