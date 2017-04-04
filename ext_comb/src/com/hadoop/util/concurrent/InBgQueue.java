package com.hadoop.util.concurrent;

/**
 * 后台异步执行的BlockingQueue
 * 
 * 
 * 
 */
public interface InBgQueue<T> {

	/**
	 * 获取队列大小。
	 * @return
	 */
	long getQueueSize();

	/**
	 * 添加对象。
	 * @param e
	 * @return
	 */
	boolean offer(T e);

	/**
	 * 通知停止。
	 */
	void signalStop();

	/**
	 * 获取容量。
	 * @return
	 */
	int getCapacity();
	
	/**
	 * 启动线程。
	 */
	void start();
	
}
