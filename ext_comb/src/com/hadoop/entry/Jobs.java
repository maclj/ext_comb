package com.hadoop.entry;

import org.apache.hadoop.conf.Configuration;

import com.hadoop.plat.util.HdfsFileUtil;
import com.hadoop.plat.util.SplitValueBuilder;

/**
 * 任务相关的常量、环境变量、命令参数等定义。
 * 
 * 
 *
 */
public final class Jobs {
    /** 任务标识,由命令行指定，用来跟踪一个hadoop命令执行的多个job id */
    public static final String JOBS_IDENTIY = "jobs.identity";
    /** 是否输出任务标识与job id的映射关系 */
    public static final String JOBS_LOG_IDENTITY = "jobs.log.identity";
    /** 任务标识与job id的日志位置 */
    public static final String JOBS_LOG_PATH = "jobs.log.path";

    /** 命令行指定的原始输入  */
    public static final String JOB_INPUT = "job.input";
    
    public static final String JOB_INPUTS = "job.inputs";
    
    /** 命令行指定的原始输出  */
    public static final String JOB_OUTPUT = "job.output";
    
    public static final String JOB_OUTPUTS = "job.outputs";
    
    /** 中间输出路径  */
    public static final String JOB_OUTPUT_MID = "job.output.mid";
    
    /** 最终输出路径  */
    public static final String JOB_OUTPUT_FINNAL = "job.output.final";
    
    /** 输出路径前缀，需要针对每个Job单独定义  */
    public static final String JOB_OUTPUT_PREFIX = "job.output.prefix";
    
    /** 命令行参数：数据日期  */
    public static final String JOB_RUN_DATE = "job.date";
    
    /** 命令行参数：数据月份 */
    public static final String JOB_RUN_MONTH = "job.month";
    
    public static final String JOB_RUN_PROVINCE = "job.province";
    
    /** 输出数据中splitKey的标识符 */
    public static final String JOB_SPLITKEY = "job.splitkey";
    
    /** 日志类型 */
    public static final String JOB_USERLOG_TYPE = "job.userlog.type";
    
    /** 输出内容的分隔符号 */
    public static final String JOB_OUTPUT_SEPARATOR = "job.output.separator";
    
    /** 环境变量(Configuration): 项目名称 */
    public static final String JOB_PROJECT = "job.name";
    
    /** 环境变量(Configuration): 项目名称 */
    public static final String JOB_PROJECT_PACKAGE = "job.package";
    
    /** 环境变量(Configuration): 合并项目 */
    public static final String JOB_COMB = "job.comb.name";
    
    /** 环境变量(Configuration): 合并项目搜索包名 */
    public static final String JOB_COMB_PACKAGE = "job.comb.package";
    
    /** 环境变量(Configuration): 合并项目各阶段定义   */
    public static final String JOB_COMB_STAGE = "job.comb.stage";
    
    /** 
     * 通过JobDefine定义管理的mappers 
     */
    public static final String JOB_COMB_MAPPERS = "job.comb.mappers";
    
    /** 
     * 通过JobDefine定义管理的combiners 
     */
    public static final String JOB_COMB_COMBINERS = "job.comb.combiners";
    
    /** 
     * 通过JobDefine定义管理的reducers 
     */
    public static final String JOB_COMB_REDUCERS = "job.comb.reducers";
    
    /** container容器内的子类加载不完整时，是抛出异常终止处理还是忽略缺失的子类，缺省为抛出异常 */
    public static final String JOB_COMB_CONTAINER_INCOMPLETE_WARN = "job.comb.container.incomplete.warn";
    
    /** 环境变量(Configuration): 日志启用 */
    public static final String ENV_LOG_ENABLED = "env.log.enabled";
    
    /** 环境变量(Configuration): 日志启用 */
    public static final String ENV_SYSOUT_ENABLED = "env.sysout.enabled";
    
    /** 环境变量(Configuration): 特殊日志输出 */
    public static final String ENV_LOG_REDIRECT = "env.log.redirect";
    
    /** 标明当前运行的任务，注意并行运行的限制  */
    public static final String JOB_CURRENT = "job.cur"; 
    
    
    public static final String getTaskId(Configuration conf) {
        String taskId = conf.get(JOBS_IDENTIY);
        if (taskId == null) {
            return "jobs." + System.currentTimeMillis();
        }
        return taskId;
    }
    
    /**
     * 获取指定key的环境变量。
     * @param conf
     * @param key
     * @return
     */
    public static final String getJobProperty(Configuration conf, String key) {
        String value = conf.get(key);
        return value;
    }
    
    /**
     * 设置对应类的key标识
     * @param conf
     * @param clazz
     * @param key
     */
    public static void setJobProperty(Configuration conf, String key, String value) {
        conf.set(key, value);
    }
    
    /**
     * 获取指定key的环境变量。
     * @param conf
     * @param key
     * @return
     */
    public static final boolean getJobBoolean(Configuration conf, String key, boolean def) {
        boolean value = conf.getBoolean(key, def);
        return value;
    }
    
    
    /**
     * 获取命令行中通过-D 传入的项目信息。
     * @param conf
     * @return
     */
    public static final String getJobProject(Configuration conf) {
        String value = getJobProperty(conf, JOB_PROJECT);
        return value;
    }
    
    /**
     * 获取命令行中通过-D 传入的包名。
     * @param conf
     * @return
     */
    public static final String getJobPackage(Configuration conf) {
        String value = getJobProperty(conf, JOB_PROJECT_PACKAGE);
        return value;
    }
    
    /**
     * 获取命令行中通过-D 传入的包名。
     * @param conf
     * @return
     */
    public static final String getCombPackage(Configuration conf) {
        String value = getJobProperty(conf, JOB_COMB_PACKAGE);
        return value;
    }
    
    /**
     * 获取命令行中通过-D 传入的各任务阶段配置。
     * @param conf
     * @return
     */
    public static final String getCombStage(Configuration conf) {
        String value = getJobProperty(conf, JOB_COMB_STAGE);
        return value;
    }
    
    /**
     * 获取命令行中通过-D 传入的合并任务定义。
     * @param conf
     * @return
     */
    public static final String getCombName(Configuration conf) {
        String value = getJobProperty(conf, JOB_COMB);
        return value;
    }
    
    /**
     * 是否启用额外的错误日志输出。
     * @return
     */
    public static final boolean isEnvLogEnabled(Configuration conf) {
        String tmp = System.getProperty(ENV_LOG_ENABLED);
        boolean value = Boolean.parseBoolean(tmp);
        if(!value && conf != null) {
            value = getJobBoolean(conf, ENV_LOG_ENABLED, false);
        }
        return value;
    }
    
    /**
     * 是否通过控制台输出提示信息。
     * @return
     */
    public static final boolean isEnvSysoutEnabled(Configuration conf) {
        String tmp = System.getProperty(ENV_SYSOUT_ENABLED);
        boolean value = Boolean.parseBoolean(tmp);
        if(!value && conf != null) {
            value = getJobBoolean(conf, ENV_SYSOUT_ENABLED, false);
        }
        return value;
    }
    
    /**
     * 获取命令行中通过-D 传入的日志重定向信息。
     * @param conf
     * @return
     */
    public static final boolean isEnvLogRedirect(Configuration conf) {
        boolean value = Boolean.parseBoolean(System.getProperty(ENV_LOG_REDIRECT));
        if(!value && conf != null) {
            value = getJobBoolean(conf, ENV_LOG_REDIRECT, false);
        }
        return value;
    }
    
//    /**
//     * 获取命令行中通过-D传入的日志重定向信息。
//     * @param conf
//     * @return
//     */
//    public static final boolean isEnvLogRedirect() {
//        boolean value = Boolean.parseBoolean(System.getProperty(ENV_LOG_REDIRECT));
//        return value;
//    }
    
    /**
     * 获取命令行中通过-D 传入的合并项目信息。
     * @param conf
     * @return
     */
    public static final String getJobComb(Configuration conf) {
        String value = getJobProperty(conf, JOB_COMB);
        return value;
    }
    
//    /**
//     * 获取命令行中通过-D 传入的合并项目信息。
//     * @param conf
//     * @return
//     */
//    public static final String getEnvCombStage(Configuration conf) {
//        String value = getJobProperty(conf, JOB_COMB_STAGE);
//        return value;
//    }
    
    public static final String getOutputMid(Configuration conf) {
        return conf.get(Jobs.JOB_OUTPUT_MID);
    }
    
    public static final String getOutputMid(Configuration conf, int index) {
        return addPath(conf.get(Jobs.JOB_OUTPUT), "mid" + index);
    }
    
    /**
     * @param conf
     * @param args
     * @return
     */
    public static final String getOutput(Configuration conf, String... args) {
        String output = conf.get(Jobs.JOB_OUTPUT);
        for(String arg : args) {
            output += arg + "/";
        }
        return output;
    }
    
    public static final String getOutputFinal(Configuration conf) {
        return conf.get(Jobs.JOB_OUTPUT_FINNAL, "");
    }
    
    public static final String getInput(Configuration conf) {
        return conf.get(Jobs.JOB_INPUT, "");
    }
    
    public static final String[] getInputs(Configuration conf) {
        return conf.getStrings(Jobs.JOB_INPUT);
    } 
    
    public static final String getOutput(Configuration conf) {
        return conf.get(Jobs.JOB_OUTPUT, "");
    }
    
    public static final String[] getOutputs(Configuration conf) {
        return conf.getStrings(Jobs.JOB_OUTPUT);
    }
    
    public static final String getOutputPrefix(Configuration conf) {
        return conf.get(Jobs.JOB_OUTPUT_PREFIX, "");
    }
    
    /**
     * 设置输出路径前缀，需要针对每个JOB单独设定。
     * @param conf
     * @param value
     */
    public static final void setOutputPrefix(Configuration conf, String value) {
        setJobProperty(conf, Jobs.JOB_OUTPUT_PREFIX, HdfsFileUtil.normalize(value));
    }
    
    /**
     * DATE 一定有值
     * @param conf
     * @return
     */
    public static String getRunDate(Configuration conf) {
        return conf.get(Jobs.JOB_RUN_DATE, "");
    }
    
    public static String getRunMonth(Configuration conf) {
        return conf.get(Jobs.JOB_RUN_MONTH, "");
    }
    
    public static String getRunProvince(Configuration conf) {
        return conf.get(Jobs.JOB_RUN_PROVINCE, "");
    }
    /**
     * 获取输出内容的分隔符号
     * @param conf
     * @return
     */
    public static String getOutputSeparator(Configuration conf) {
        return conf.get(Jobs.JOB_OUTPUT_SEPARATOR, "|");
    }
    
    /**
     * 设置对应类的key标识
     * @param conf
     * @param clazz
     * @param key
     */
    public static void setSplitKeyByClass(Configuration conf, Class<?> clazz, String key) {
        String name = JOB_SPLITKEY + "_" + clazz.getCanonicalName();
        conf.set(name, key);
    }
    
    /**
     * 获取对应类的key标识
     * @param conf
     * @param clazz
     * @param splitKey
     */
    public static String getSplitKeyByClass(Configuration conf, Class<?> clazz) {
        String name = JOB_SPLITKEY + "_" + clazz.getCanonicalName();
        return conf.get(name);
    }
    
    /**
     * 容器内子类加载不完全时是否抛出异常
     * @param conf
     * @return
     */
    public static boolean isWarnWhenContainIncomplete(Configuration conf) {
        return conf.getBoolean(Jobs.JOB_COMB_CONTAINER_INCOMPLETE_WARN, true);
    }
    
    /**
     * 根据命令行参数和conf初始化，由入口类负责调用。
     * @param conf
     * @param jco
     */
    public static void init(Configuration conf, JobCommandOptions jco) {
        
        if(jco.getCity() != null) {
            conf.set(Jobs.JOB_RUN_PROVINCE, jco.getCity());
            print(conf, Jobs.JOB_RUN_PROVINCE, jco.getCity());
        }
        // 日期配置
        if(jco.getDate() != null) {
            conf.set(Jobs.JOB_RUN_DATE, jco.getDate());
            print(conf, Jobs.JOB_RUN_DATE, jco.getDate());
            //appendDir = jco.getDate();
        }
        
        // 月份配置
        String month = resolveMonth(jco.getDate());
        if(!"".equals(month)) {
            conf.set(Jobs.JOB_RUN_MONTH, month);
            print(conf, Jobs.JOB_RUN_MONTH, month);
            //appendDir = month + File.pathSeparator + appendDir;
        }
        
        // 多输入路径，只有配置多个-i才会进入
        if(jco.getInputs() != null && jco.getInputs().length > 1) {
            SplitValueBuilder svb = new SplitValueBuilder(",");
            for(String i : jco.getInputs()) {
                svb.add(HdfsFileUtil.normalize(i));
            }
            conf.set(Jobs.JOB_INPUTS, svb.build() );
            print(conf, Jobs.JOB_INPUTS, conf.get(Jobs.JOB_INPUTS));
        } 
        // 单输入路径，任何时候都保证有值
        if(jco.getInput() != null) {
            conf.set(Jobs.JOB_INPUT, HdfsFileUtil.normalize(jco.getInput()) );
            print(conf, Jobs.JOB_INPUT, conf.get(Jobs.JOB_INPUT));
        }
        
        // 多输出路径，只有配置多个-o才会进入
        if(jco.getOutputs() != null && jco.getOutputs().length > 1) {
            SplitValueBuilder svb = new SplitValueBuilder(",");
            for(String o : jco.getOutputs()) {
                svb.add(HdfsFileUtil.normalize(o));
            }
            conf.set(Jobs.JOB_OUTPUTS, svb.build() );
            print(conf, Jobs.JOB_OUTPUTS, conf.get(Jobs.JOB_OUTPUTS));
        } 
        // 单输出路径，任何时候都保证有值
        if(jco.getOutput() != null) {
            conf.set(Jobs.JOB_OUTPUT, HdfsFileUtil.normalize(jco.getOutput()));
            print(conf, Jobs.JOB_OUTPUT, conf.get(Jobs.JOB_OUTPUT));
            
            conf.set(Jobs.JOB_OUTPUT_MID, addPath(jco.getOutput(), "mid"));
            // print(Jobs.JOB_OUTPUT_MID, conf.get(Jobs.JOB_OUTPUT_MID));
            
            conf.set(Jobs.JOB_OUTPUT_FINNAL, addPath(jco.getOutput(), "final"));
            // print(Jobs.JOB_OUTPUT_FINNAL, conf.get(Jobs.JOB_OUTPUT_FINNAL));
        }
        
        // 日志启用标识，如果使用的是system.getProperty同步到各集群
        if(isEnvLogEnabled(conf)) {
            conf.setBoolean(Jobs.ENV_LOG_ENABLED, true);
        }
        if(isEnvSysoutEnabled(conf)) {
            conf.setBoolean(Jobs.ENV_SYSOUT_ENABLED, true);
        }
        if(isEnvLogRedirect(conf)) {
            conf.setBoolean(Jobs.ENV_LOG_REDIRECT, true);
        }
    }
    
    private static void print(Configuration conf, String property, String value) {
        if(Jobs.isEnvSysoutEnabled(conf)) {
            System.out.println(String.format("param(%s): %s", property, value));
        }
    }
    
    private static String addPath(String src, String add) {
        src = HdfsFileUtil.normalize(src);
        return src + add + "/";
    }
    
    
    /**
     * 简单实现，不做格式校验，
     * 仅支持形如2016-12-07和20161207 两种形式。
     * @param date
     * @return
     */
    private static String resolveMonth(String date) {
        // 目前支持的格式为2016-12-07
        if(date == null) {
            return "";
        }
        if (date.contains("-")) {
            if (date.length() == 10) {
                return date.substring(0, 7);
            }
            if (date.length() == 7) {
                return date;
            }
        } else {
            if(date.length() == 8) {
                return date.substring(0, 6);
            }
            if (date.length() == 6) {
                return date;
            }
        }
        return ""; 
    }
    
}
