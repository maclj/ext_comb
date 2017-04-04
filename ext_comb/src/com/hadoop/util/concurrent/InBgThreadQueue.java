package com.hadoop.util.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 直接使用出队列的线程进行任务的执行。 由调用者指定对应的线程个数。
 *
 * @param <T>
 * 
 * 
 */
public class InBgThreadQueue<T> implements InBgQueue<T> {

	/** 日志。 */
	private Log log = LogFactory.getLog(InBgThreadQueue.class);

	/** 数据队列。 */
	private BlockingQueue<T> theQueue = null;

	/** 消费数据队列的Command */
	private Command<T> command = null;

	/** 默认数据队列的容量。 */
	private int capacity = 500;

	/** 出队列线程数。 */
	private int cosumeCount = 1;
	
	/** 持有所有后台线程的引用 */
	private List<Thread> threadPool;

	/** 缺省的线程名称 */
	private String threadName = "BGThread";

	/**
	 * 运行标识。
	 */
	private volatile boolean running = true;
	
	/**
	 * 构造函数，用于独立线程池。
	 * 
	 * @param command
	 * @param capacity
	 * @param nThreads
	 * @param workCount
	 * @param threadName
	 */
	public InBgThreadQueue(Command<T> command, int capacity, int consume, String threadName) {
		this.threadName = threadName;
		this.command = command;
		this.capacity = capacity;
		this.theQueue = new LinkedBlockingQueue<T>(capacity);
		this.cosumeCount = consume;
		this.threadPool = new ArrayList<>(this.cosumeCount);
	}

	/**
	 * 构造函数，用于独立线程池。
	 * 
	 * @param command
	 *            执行的命令
	 * @param threadName
	 *            线程名称
	 * @param capacity
	 *            队列容量
	 * @param consume
	 *            消费的线程数量
	 */
	public InBgThreadQueue(Command<T> command, String threadName, int capacity,
			int consume) {
		this.threadName = threadName;
		this.command = command;
		this.capacity = capacity;
		this.theQueue = new LinkedBlockingQueue<T>(capacity);
		this.cosumeCount = consume;
		this.threadPool = new ArrayList<>(this.cosumeCount);
	}

	/**
	 * 构造函数， 不推荐使用此构造函数，都是默认值。
	 * 
	 * @param command
	 */
	public InBgThreadQueue(Command<T> command, String threadName) {
		this.threadName = threadName;
		this.command = command;
		this.theQueue = new LinkedBlockingQueue<T>(capacity);
		this.threadPool = new ArrayList<>(this.cosumeCount);
	}

	/**
	 * 启动线程。
	 */
	public void start() {
		// 启动内部出队列线程
		Thread t;
		for (int i = 0; i < this.cosumeCount; i++) {
			t = new MultiDequeueThread();
			t.setName(this.threadName + "-" + t.getId());
			this.threadPool.add(t);
			t.start();
		}
	}

	/** returns the size of the queue. */
	public long getQueueSize() {
		return theQueue.size();
	}

	/** Queue a trap to be processed. */
	public boolean offer(T e) {
		return theQueue.offer(e);
	}

	/** stop the processor thread. */
	public void signalStop() {
		running = false;
		// 非服务形式下使用,需要主动关闭掉后台线程.
		for(Thread t : this.threadPool) {
			try {
				t.interrupt();
			} catch(Exception e) {
				//ignored
			}
		}
		this.threadPool.clear();
	}

	public int getCapacity() {
		return capacity;
	}

	/**
	 * 内部线程，直接访问。
	 * 
	 */
	class MultiDequeueThread extends Thread {
		MultiDequeueThread() {
			setDaemon(true);
		}

		/**
		 * Thread run() method
		 */
		public void run() {
			while (running) {
				try {
					final T task = theQueue.take();
					if (task == null) {
						// 不可能发生
						log.warn("take null value from queue.");
						continue;
					}
					if (log.isTraceEnabled()) {
						log.trace("invoke command.doCommand in background, "
								+ command.getClass().getName());
					}
					// 实际执行命令。
					command.newCommand().doCommand(task);
					if (log.isTraceEnabled()) {
						log.trace("invoke command.doCommand in background,  succeeded.");
					}

				} catch (InterruptedException e) {
					log.trace("Interrupted, " + e);
				} catch (Exception e) {
					log.warn("running = " + running, e);
				}
			} // while...
			// log.info("Quit thread " + getName());
		}

	}
} // end
