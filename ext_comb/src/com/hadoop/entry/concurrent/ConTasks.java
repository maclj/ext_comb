package com.hadoop.entry.concurrent;

import java.io.Closeable;

/**
 * 任务池接口。
 * 
 *
 */
public interface ConTasks extends Closeable {
	
	/**
	 * 运行任务池。
	 */
	void execute();
	
	/**
	 * 释放任务池资源。
	 */
	void close();
}
