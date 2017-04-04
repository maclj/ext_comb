package com.hadoop.entry;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.hadoop.util.DateUtil;
import com.hadoop.util.Logger;

/**
 * 运行入口，用于解决类装载问题，需要业务继承，避免需要单独加载的处理。
 * 
 *
 */
public class JobRunner {
    
    
    /**
     * 解析命令行参数。
     * 
     * @param conf
     * @param args
     * @return
     * @throws Exception
     */
    protected JobCommandOptions parseArgs(Configuration conf, String[] args) throws Exception {
        // 命令行参数解析
        JobCommandOptions jco = new JobCommandOptions(conf, args);

        // 帮助则打印提示
        if (jco.hasOption(JobCommandOptions.OPTION_HELP)) {
            JobCommandOptions.printGenericCommandUsage(System.out);
            return null;
        }

        // 用于本地测试校验，没有输入路径配置的情况下，直接输出参数提示。
//        if (jco.getInput() == null) {
//            JobCommandOptions.printGenericCommandUsage(System.out);
//            return null;
//        }

        String project = Jobs.getJobProject(conf);
        String pack = Jobs.getJobPackage(conf);
        if(pack == null) {
            pack = "com.hadoop,com.carbon,mapreduce"; // 注意：这里缺省设置了3个包
            Jobs.setJobProperty(conf, Jobs.JOB_PROJECT_PACKAGE, pack);
        }
        
        Logger.print(String.format("param(job.name): %s", project));
        // System.out.println(String.format("param(package): %s", pack));
        if (project == null || pack == null) {
            System.out.println(String.format("invalid paramters(empty project or pack)."));
            return null;
        }

        return jco;
    }

    /**
     * 构造并运行任务
     * @param conf
     * @param jco
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws InterruptedException
     * @return errorCount 错误任务计数
     */
    protected List<Project> runJobs(Configuration conf, JobCommandOptions jco)
            throws ClassNotFoundException, IOException, InterruptedException {
        
        List<Project> projs = ClassFinder.findProjectsByJobConf(Jobs.getJobPackage(conf), Jobs.getJobProject(conf));
        if (projs.size() == 0) {
            System.out.println("No Jobs.");
            System.exit(0);
        }

        ProjectRunner pr = new ProjectRunner(conf, jco, projs);
        pr.runJobs();
        return projs;
    }

    /**
     * 实际运行入口。
     * @param args
     * @throws Exception
     */
    public void run(String[] args) throws Exception {
        
        // 任务配置
        Configuration conf = new Configuration();

        // 初始化日志
        Logger.init(conf);
        
        // 命令行参数解析
        JobCommandOptions jco = parseArgs(conf, args);
        if (jco == null) {
            return;
        }

        // 初始化环境变量
        Jobs.init(conf, jco);
        
        // 运行任务
        List<Project> projs = runJobs(conf, jco);
        
        int errorJobs = 0;
        for(Project proj : projs) {
            proj.print();
            errorJobs += proj.getCountFailure();
        }    
        System.out.println(String.format("[%s]All Jobs Done.", DateUtil.currentDate()));
        System.exit(errorJobs); // 结果是失败job总个数，为0则为成功。
        
    }
    
    public static void main(String[] args) throws Exception {
        new JobRunner().run(args);
    }
}
