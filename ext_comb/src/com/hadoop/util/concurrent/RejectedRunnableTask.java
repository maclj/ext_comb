package com.hadoop.util.concurrent;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * 加入ThreadPoolExecutor执行的线程建议同时实现此接口，
 * 以实现不同的拒绝回调处理，如不同的日志方式。
 *
 * 
 */
public interface RejectedRunnableTask extends Runnable {
	
	/**
	 * 传入executor本身，可以不使用。
	 * @param executor
	 */
	void rejected(ThreadPoolExecutor executor);
}
