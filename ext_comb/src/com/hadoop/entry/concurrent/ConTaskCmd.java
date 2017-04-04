package com.hadoop.entry.concurrent;

/**
 * 实际处理任务数据的实现，由业务开发者负责实现。
 * 
 * 
 *
 */
public interface ConTaskCmd<T> {
	
	/**
	 * 执行数据。
	 * @param data
	 */
	void execute(T data, boolean isLast);
}
