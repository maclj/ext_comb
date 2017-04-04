package com.hadoop.entry;

/**
 * 统一任务调用入口。
 * 
 * 
 *
 */
public class Main {
    
    public static void main(String[] args) throws Exception {
        new JobRunner().run(args);
    }

}
