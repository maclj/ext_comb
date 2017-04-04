package com.hadoop.entry;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import com.hadoop.entry.concurrent.ConTaskCmdAdapter;
import com.hadoop.entry.concurrent.ConTasks;
import com.hadoop.entry.concurrent.ConTasksBuilder;
import com.hadoop.util.DateUtil;
import com.hadoop.util.concurrent.InterruptedRuntimeException;

/**
 * 支持多个项目的并行运行（单一项目之间的Job运行顺序仍然依据jobSeq和sync的配置）
 * 
 * 缺省为串行运行。
 * 
 * 
 *
 */
public class ProjectRunner {
    
    /** 运行参数的前缀 */
    private static final String PROJECT_PARAM_PREFIX = "jobs.%s";
    
    /** 是否为并行处理，缺省为false  */
    // public static final String PARAM_PARALLEL = "parallel";
    
    /** 超时时长，缺省为0，无超时设置  */
    public static final String PARAM_TIMEOUT = "timeout";
    
    /** 并行的线程个数，缺省为0，串行 */
    public static final String PARAM_THREADS = "threads";
    
    /** 每个线程启动的间隔，缺省为10秒. */
    public static final String PARAM_DELAY = "delay";
    
    /** 输出分隔线 */
    private static final String PRINT_LINE = "================================================================";
    
    /** 支持的运行期参数设置  */
    public static final String[] PARAMS = new String[]{PARAM_TIMEOUT, PARAM_THREADS, PARAM_DELAY}; //PARAM_PARALLEL
    
    /** 待运行的项目 */
    private List<Project> projects;
    
    /** 上下文 */
    private Configuration conf;
    
    /** 命令行参数 */
    private JobCommandOptions jco;
    
    /** 存放运行期参数键值对，注意：未携带前缀信息 且只包含存在定义的参数配置 */
    private Map<String, String> params;
    
    /**
     * 构造函数
     * @param conf
     * @param projects
     */
    public ProjectRunner(Configuration conf, JobCommandOptions jco, List<Project> projects) {
        this.projects = projects;
        this.conf = conf;
        this.jco = jco;
        this.params = new HashMap<String, String>();
        
        String paramName = null;
        String paramValue = null;
        for(String m : PARAMS) {
            paramName = String.format(PROJECT_PARAM_PREFIX, m);
            paramValue = this.conf.get(paramName, null);
            if(paramValue == null) {
                continue; // 运行期间无此定义
            }
            this.params.put(m, paramValue);
        }
    }
    
    /**
     * 是否为并行运行多项目，缺省为false
     * @return
     */
    public boolean isParallel() {
        if(this.projects.size() < 2) {
            return false; // 只有1个任务，忽略参数
        }
        if(getThreads() > 1) {
            return true; // 线程数大于1则认为需要并行处理
        }
        return false;
    }
    
    /**
     * 获取缺省运行的线程数量，缺省为4.
     * @return
     */
    public int getThreads() {
        int defaultValue = 0;
        String paramValue = this.params.get(PARAM_THREADS);
        if(paramValue == null) {
            return defaultValue;
        }
        try {
            return Integer.valueOf(paramValue);
        } catch(Exception e) {
            return defaultValue;
        }
    }
    
    /**
     * 获取缺省的超时时间，缺省为0，没有超时设置.
     * @return
     */
    public int getTimeout() {
        int defaultValue = 0;
        String paramValue = this.params.get(PARAM_TIMEOUT);
        if(paramValue == null) {
            return defaultValue;
        }
        try {
            return Integer.valueOf(paramValue);
        } catch(Exception e) {
            return defaultValue;
        }
    }
    
    /**
     * 获取线程启动的间隔。
     * @return
     */
    public int getDelay() {
        int defaultValue = 10;
        String paramValue = this.params.get(PARAM_DELAY);
        if(paramValue == null) {
            return defaultValue;
        }
        try {
            return Integer.valueOf(paramValue);
        } catch(Exception e) {
            return defaultValue;
        }
    }
    
    /**
     * 运行所有的项目
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws InterruptedException
     */
    public void runJobs() throws ClassNotFoundException, IOException, InterruptedException {
        // 串行
        if (!isParallel()) {
            for (Project p : this.projects) {
                run(p);
            }
        } else {
            // 定义并发任务池
            ConTasks tasks = null;
            try {
                int threads = getThreads();
                int timeout = getTimeout();
                int delay =  getDelay();
                ConTasksBuilder<Project> builder = new ConTasksBuilder<Project>()
                        .blocking(true)
                        .once(true)
                        .threads(threads)
                        .timeout(timeout)
                        .delay(delay)
                        .addDatas(this.projects)
                        .addCommand(new ConTaskCmdAdapter<Project>() {
                            @Override
                            public void execute(Project p, boolean isLast) {
                                try {
                                    run(p);
                                } catch (ClassNotFoundException e) {
                                    throw new InterruptedRuntimeException(e);
                                } catch (IOException e) {
                                    throw new InterruptedRuntimeException(e);
                                } catch (InterruptedException e) {
                                    throw new InterruptedRuntimeException(e);
                                }
                            }
                        });

                // 返回并发任务池
                tasks = builder.build();
                // 运行，project之间并行，主线程等待所有proj运行结束后退出
                tasks.execute();
                
            } finally {
                // 释放资源。
                if(tasks != null) {
                    tasks.close();
                }
            }
        }
    }
    
    /**
     * 兼容原处理。
     * @return
     */
    public List<JobDefine> getJobDefines() {
        List<JobDefine> jobs = new LinkedList<JobDefine>();
        for(Project p: this.projects) {
            jobs.addAll(p.getJobs());
        }
        return jobs;
    }
    
    public List<Project> getProjects() {
        return projects;
    }

    /**
     * 运行一个项目下的所有job
     * @param proj
     * @throws InterruptedException 
     * @throws IOException 
     * @throws ClassNotFoundException 
     */
    private void run(Project proj) throws ClassNotFoundException, IOException, InterruptedException {
        
        JobBuilder jobBuilder = null;
        JobDefineResolver jdr = null;
        Job job = null;
        Job lastJob = null;
        
        List<JobDefine> jds = proj.getJobs();
        if(jds.size() == 0) {
            System.out.println(String.format("Project(%s) conatins no jobs.", proj.getName()));
            return;
        }
        
        Configuration copy = new Configuration(this.conf); //copy
        for (JobDefine jd : jds) {
            jdr = new JobDefineResolver(copy, jd);
            jobBuilder = new JobBuilder(copy, jd, this.jco.getRemainingArgs(), jdr, lastJob);
            if (!jobBuilder.isOk()) {
                System.out.println("Job init failed: " + JobDefineHelper.getJobDesc(jd));
                continue;
            }
            String jobName = jobBuilder.getJobDesc();
            job = jobBuilder.build();
            lastJob = job;// 保留上一个job的引用 ，无论是否为null。
            
            System.out.println(PRINT_LINE);
            if (job == null) {
                System.out.println(String.format("[%s]Job ignored: %s", DateUtil.currentDate(), jobName));
                System.out.println(PRINT_LINE);
                continue;
            }

            System.out.println(String.format("[%s]Job starting: %s", DateUtil.currentDate(), jobName));
            System.out.println(PRINT_LINE);

            boolean isSync = jdr.parse("sync", jd.sync());
            proj.addTotal();
            if (isSync) {
                long start = System.currentTimeMillis();
                boolean result = job.waitForCompletion(true);
                long spend = System.currentTimeMillis() - start;
                
                if(result) {
                    proj.addCountSuccess();
                }
                proj.addSpend(spend);
                
                System.out.println(PRINT_LINE);
                System.out.println(String.format("[%s][%s][%9d]Job finished: %s.", DateUtil.currentDate(), (result ? "S" : "F"), spend, jobName));
            } else {
                job.submit();
                proj.addCountSubmitted();
                System.out.println(String.format("[%s]Job submited: %s", DateUtil.currentDate(), jobName));
            }
            
            System.out.println(PRINT_LINE);
        }
    }

    public static abstract class JobListener {
        public void jobLoaded(String jobId, String jobName) {

        }

        public void jobSubmitted(String jobId, String jobName) {

        }

        public void jobSuccessed(String jobId, String jobName) {

        }

        public void jobFailed(String jobId, String jobName) {

        }
    }
}
