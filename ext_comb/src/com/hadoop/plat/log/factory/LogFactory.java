package com.hadoop.plat.log.factory;

public interface LogFactory {

	/**
	 * 通过原始日志内容解析出日志对象。
	 * @param value
	 * @return
	 */
	Log parse(String value);

}
