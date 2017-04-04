package com.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;

import com.hadoop.entry.ClassFinder;
import com.hadoop.entry.Jobs;
import com.hadoop.entry.comb.CombContainer;


/**
 * 根据Job定义和 Configuration，解析Container相关配置。
 * 
 * 样例：
 * -Dcontainer.containerName.jobName.jobSeq=a.class,b.class
 * -Dcontainer.containerName=a.class,b.class
 * 
 * 
 *
 */
public class ContainerResolver {
    
    /** 运行参数的前缀,形如container.containerName.jobName.jobSeq */
    // public static final String JOB_PARAM_PREFIX = "container.%s.%s";
    
    /** 运行参数的前缀,形如container.containerName */
    public static final String JOB_PARAM_PREFIX_GLOBAL = "container.%s";
    
    public static final byte TYPE_MAPPER = 0;
    
    public static final byte TYPE_COMBINER = 1;
    
    public static final byte TYPE_REDUCER = 2;

    /** 上下文 */
    private Configuration conf;
    
    /** 容器类定义 */
    private CombContainer c;
    
    /** 优先获取job相关参数 */
    // private String keyJob ;
    
    /** 获取全局参数 */
    private String keyGlobal;
    
    /** 是否抛出异常  */
    private boolean showWarn;
    
    /** 0:mapper 1:combiner; 2:reducer */
    private byte type;
    
    public ContainerResolver() {
    }
    
//    /**
//     * 
//     * @param conf
//     * @param clazz 容器类
//     * @param jobNameSeq 自行组装的jobName.jobSeq描述
//     */
//    public ContainerResolver(Configuration conf, Class<?> clazz, String jobNameSeq) {
//        
//        this.conf = conf;
//        // 获取类上的annotation定义
//        CombContainer c = clazz.getAnnotation(com.hadoop.entry.comb.CombContainer.class);
//        if(c == null) {
//            return;
//        }
//        this.c = c;
//        String containerName = c.name(); // container name
//        if(jobNameSeq != null) {
//            this.keyJob = String.format(JOB_PARAM_PREFIX, containerName, jobNameSeq);
//        }
//        this.keyGlobal = String.format(JOB_PARAM_PREFIX_GLOBAL, containerName);
//    }
//    
//    /**
//     * @param conf
//     * @param clazz 容器类
//     */
//    public ContainerResolver(Configuration conf, Class<?> clazz) {
//        this(conf, clazz, conf.get(Jobs.JOB_CURRENT, null));
//    }
    
    public ContainerResolver withConf(Configuration conf) {
        this.conf = conf;
        return this;
    }
    
    public ContainerResolver withClass(Class<?> clazz, String jobNameSeq) {
        // 获取类上的annotation定义
        CombContainer c = clazz.getAnnotation(com.hadoop.entry.comb.CombContainer.class);
        if(c == null) {
            return this;
        }
        this.c = c;
        String containerName = c.name(); // container name
//        if(jobNameSeq != null) {
//            this.keyJob = String.format(JOB_PARAM_PREFIX, containerName, jobNameSeq);
//        }
        this.keyGlobal = String.format(JOB_PARAM_PREFIX_GLOBAL, containerName);
        return this;
    }
    
    public ContainerResolver withClass(Class<?> clazz) {
        return withClass(clazz, null);
    }
    
    public ContainerResolver withWarn(boolean showWarn ) {
        this.showWarn = showWarn;
        return this;
    }
    
    public ContainerResolver withMapper() {
        this.type = TYPE_MAPPER;
        return this;
    }
    
    public ContainerResolver withCombiner() {
        this.type = TYPE_COMBINER;
        return this;
    }
    
    public ContainerResolver withReducer() {
        this.type = TYPE_REDUCER;
        return this;
    }
    
    
    /**
     * container管理类未定义
     * @param args
     * @return
     */
	public static boolean isUndefined(String[] args) {
		if (args == null) {
			return true;
		}
		if (args.length == 1 && args[0].equals("")) {
			return true;
		}
		return false;
	}
	
	/**
	 * container管理类未定义
	 * @param args
	 * @return
	 */
	public static boolean isUndefined(Class<?>[] args) {
		if (args == null) {
			return true;
		}
		if ( args.length == 1 && (args[0] == Object.class)) {
			return true;
		}
		return false;
	}
    
	/**
	 * 获取运行参数或者Annotation中定义的被管理类。<br>
	 * 
	 * 顺序为：<br>
	 * 1、-Dcontainer.containerName 形式定义的管理类（运行参数） <br>
	 * 2、-Djob.jobName.jobSeq.container*** 形式定义的管理类（运行参数）<br>
	 * 3、jobDefine 中定义的管理类<br>
	 * 4、annotation中 values 定义的类名称（annotation）<br>
	 * 5、annotation中clazzes定义的类定义（annotation）<br>
	 * 
	 * @param showWarn
	 * @return
	 */
    public Class<?>[] fetchClasses() {
    	//  兼容之前的优先级，
    	String[] clazzArray = parseClasses(); 
    	
    	// 第6优先获取类上的annotation(clazzes)定义
    	Class<?>[] clazzes = null;
		if (!isUndefined(clazzArray)) {
			clazzes = new Class<?>[clazzArray.length];

			Class<?> cl = null;
			for (int i = 0; i < clazzArray.length; i++) {
				String clazzName = clazzArray[i];
				cl = ClassFinder.loadClass(clazzName);
				if (cl == null && this.showWarn) {
					throw new IllegalArgumentException(String.format("can't loadClass of %s.", clazzName));
				}
				clazzes[i] = cl;
			}
		} else {
			clazzes = this.c.clazzes();
		}
		return clazzes;
    	
    }
    
    private String[] parseClasses() {
        return parseClasses(null);
    }
    
    /**
     * 
     * @param paramName
     * @param defaultValue
     * @return
     */
    private String[] parseClasses(String name) {
        String value = null;
        
        // 第1优先获取-Dcontainer.containerName.jobName.jobSeq（脚本指定）
//        if (this.keyJob != null) {
//        	// 考虑的场景是，该Container被多个JOB使用且同时\先后运行，但是在运行期间仅需要修改其中一个Container的配置
//            value = this.conf.get(this.keyJob, null); 
//        }
        // 第1优先获取-Dcontainer.containerName（脚本指定）
        if (value == null && this.keyGlobal != null) {
            value = this.conf.get(this.keyGlobal, null);
        }
        // 第2优先获取-Djob.comb.mappers/combiners/reducers(程序自动设置，非脚本输入)
        if(value == null) {
            if(this.type == TYPE_MAPPER) {
                value = this.conf.get(Jobs.JOB_COMB_MAPPERS, null);
            } else if(this.type == TYPE_COMBINER) {
                value = this.conf.get(Jobs.JOB_COMB_COMBINERS, null);
            } else {
                //reducers
                value = this.conf.get(Jobs.JOB_COMB_REDUCERS, null);
            }
        }
        // 第3优先获取-Djob.comb.container，取消支持
//        if (value == null) {
//            // 兼容原处理
//            // 获取-D 参数中的配置，形如-D
//            // job.comb.container="container1=map1,map2;container2=mapA,mapB"
//            String param = Jobs.getJobProperty(conf, Jobs.JOB_COMB_CONTAINER);
//            if (param != null) {
//                String[] keyValues = param.split(";");
//                Map<String, String> map = new HashMap<String, String>();
//                if (keyValues != null) {
//                    String[] cfg = null;
//                    for (String row : keyValues) {
//                        cfg = row.split("=");
//                        if (cfg.length == 2) {
//                            map.put(cfg[0], cfg[1]);// value中保存了逗号分隔的多个配置
//                        }
//                    }
//                }
//                if(name != null) {
//                    value = map.get(name);
//                }
//                if(value == null && this.c != null) {
//                    value = map.get(this.c.name());
//                }
//            }
//        }
        

        // 第5优先获取类上的annotation(values)定义
        String[] clazzArray = null;
        if (value != null) {
            clazzArray = value.split(","); // 使用时拆分多条配置
        } else if(this.c != null) {
            clazzArray = this.c.values();
        }
        return clazzArray;
    }
    
}
