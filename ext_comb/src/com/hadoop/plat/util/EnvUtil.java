package com.hadoop.plat.util;

import org.apache.commons.lang.StringUtils;

public class EnvUtil {

	/**
	 * hadoop程序是否运行在本地测试环境中
	 * @return
	 */
	public static boolean isOnLocal() {
		return System.getProperty("env.mr.local") != null;
	}
	
	/**
	 * 自动构造配置文件的上下文路径。
	 * @param confPath 输入形如 conf/project/abc.xml,注意开头不带目录分隔符号
	 * @return
	 */
	public static String getPath(String confPath) {		
		if(StringUtils.isBlank(confPath)){
			return confPath;
		}
		
		// hadoop 集群环境
		if (!isOnLocal()) {
			String[] paths = confPath.split("/");
			String path = paths[paths.length - 1];
			return "./" + path;
		}
		// hadoop 单机测试环境
		// 注意：必须以/做为目录分隔符，并且目录最后也需要包含/
		return HdfsFileUtil.normalize(System.getProperty("env.mr.local")) + confPath;
	}
	
}
