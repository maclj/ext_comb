package com.hadoop.entry.comb;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import com.hadoop.entry.ClassFinder;
import com.hadoop.entry.JobCommandOptions;
import com.hadoop.entry.JobDefine;
import com.hadoop.entry.JobRunner;
import com.hadoop.entry.Jobs;

@Deprecated
public class CombRunner extends JobRunner {

    /**
     * 根据名称查找对应匹配的CombJob定义
     * @param cjs
     * @param combJobName
     * @return
     */
    private boolean hasCombJob(CombJob[] cjs, String combJobName) {
        for(CombJob cj : cjs) {
            if(cj.name().equals(combJobName)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * 构造并运行任务
     * @param conf
     * @param jco
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws InterruptedException
     */
    protected void runCombJobs(Configuration conf, JobCommandOptions jco)
            throws ClassNotFoundException, IOException, InterruptedException {

        // 获取剩余的命令行参数
        // String[] remainingArgs = jco.getRemainingArgs();
        
        // 需要搜索的包名定义
        String combPackage = Jobs.getCombPackage(conf);
        if(combPackage == null || combPackage.length() == 0) {
            // combPackage = "com,mapreduce"; // 缺省搜索包名
        }
        // 获取指定合并的项目名称
        String combJobName = Jobs.getJobComb(conf);
        // 找到所有包含混合项目定义的类
        Set<Class<?>> combJobs = ClassFinder.findClassByAnnotation(combPackage, CombJobs.class);
       
        boolean ignored = false;
        for(Class<?> clazz : combJobs) {
            CombJobs cjbs = clazz.getAnnotation(CombJobs.class);
            if(cjbs == null) {
                continue;
            }
            CombJob[] cjs = cjbs.values();
            if(cjs == null) {
                continue;
            }
            // 名称不匹配
            ignored = !hasCombJob(cjs, combJobName);
            if(ignored) {
                continue;
            }
            
            JobDefine jd = clazz.getAnnotation(JobDefine.class);
            if(jd == null) {
                continue; // 必须同时出现
            }
            // 如果需要过滤掉的项目，则排除，由运行参数制定。
//            if(!ignored) {
//                continue;
//            }
            
            
            
        }
        
        
        // 查找所有的CombStage定义
        String combStage = Jobs.getCombStage(conf);
        if(combStage == null || combStage.length() == 0) {
            combStage = "stage.0,stage.1,stage.2"; // 缺省3个任务,必须有顺序
        }
        Class<?>[] combStages = ClassFinder.findClassByCombStage(combPackage, combStage);
        if(combStages == null) {
            System.out.println("CombStage doesnot exist.");
            return;
        }
        // 
        // int stageLength = combStages.length;
        
        JobDefine jd = null;
        for (Class<?> clazz : combStages) {
            jd = clazz.getAnnotation(JobDefine.class);
            if(jd == null) {
                continue;
            }
            jd.mapperClass();

        }
        
        
        
        

//        // 任务
//        List<JobDefine> jds = ClassFinder.findClassByJobConf(Jobs.getEnvPackage(conf), Jobs.getEnvProject(conf));
//        JobBuilder jobBuilder = null;
//        Job job = null;
//        int lastJobSeq = -1;
//        for (JobDefine jd : jds) {
//            // 如果与之前的任务ID重复，说明有定义重复的情况。
//            if (lastJobSeq == jd.jobSeq()) {
//                System.out.println("duplicated jobcof: " + JobDefineHelper.getJobDesc(jd));
//                continue;
//            }
//            lastJobSeq = jd.jobSeq();
//
//            jobBuilder = new JobBuilder(conf, jd, remainingArgs);
//            job = jobBuilder.build();
//            if (job == null) {
//                System.out.println("invalid jobcof: " + JobDefineHelper.getJobDesc(jd));
//                break;
//            }
//
//            job.waitForCompletion(true);
//        }
    }

    /**
     * 实际运行入口。
     * 
     * @param args
     * @throws Exception
     */
    public void run(String[] args) throws Exception {

        // 任务配置
        Configuration conf = new Configuration();

        // 命令行参数解析
        JobCommandOptions jco = parseArgs(conf, args);
        if (jco == null) {
            return;
        }

        // 初始化环境变量
        Jobs.init(conf, jco);
        
        // 运行任务
        runJobs(conf, jco);

        System.out.println("All Jobs Done.");
        System.exit(0);
    }
}
