package com.hadoop.util.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 可定制工作队列长度的Executors封装。
 * 规避缺省Executors中无法控制接收任务的队列的长度。
 * 
 * 
 */
public final class LimitExecutors {
	private LimitExecutors() {
	}

	/**
	 * 同Executors.newFixedThreadPool(int nThreads)
	 * @param nThreads
	 * @param capacity
	 * @return
	 */
	public static ExecutorService newFixedThreadPool(int nThreads, int capacity) {
		return new ThreadPoolExecutor(nThreads, nThreads, 0L,
				TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(
						capacity));
	}

	/**
	 * 同Executors.newFixedThreadPool(int nThreads, ThreadFactory threadFactory)
	 * @param nThreads
	 * @param capacity
	 * @param threadFactory
	 * @return
	 */
	public static ExecutorService newFixedThreadPool(int nThreads,
			int capacity, ThreadFactory threadFactory) {
		return new ThreadPoolExecutor(nThreads, nThreads, 0L,
				TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(
						capacity), threadFactory, new RejectedExecutionHandlerImpl());
	}
	
	/**
	 * 
	 * @param coreThreads
	 * @param maxThreads
	 * @param idle
	 * @param capacity
	 * @param threadFactory
	 * @return
	 */
	public static ExecutorService newFixedThreadPool(int coreThreads, int maxThreads,
			long idle, int capacity, ThreadFactory threadFactory) {
		return new ThreadPoolExecutor(coreThreads, maxThreads, idle,
				TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(
						capacity), threadFactory, new RejectedExecutionHandlerImpl());
	}
}
