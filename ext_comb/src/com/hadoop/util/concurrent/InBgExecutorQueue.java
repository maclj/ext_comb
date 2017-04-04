package com.hadoop.util.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 任务出队列后，使用 ExecutorService线程池进行任务的调度执行。 出队列的线程单独管理，由调用者指定对应的线程个数。
 *
 * @param <T>
 * 
 * 
 */
public class InBgExecutorQueue<T> implements InBgQueue<T> {

	/** 日志。 */
	private Log log = LogFactory.getLog(InBgExecutorQueue.class);

	/** 消费数据队列的Command */
	private Command<T> command = null;

	/** 线程池。 */
	private ExecutorService executor;

	/**
	 * 运行标识。
	 */
	private volatile boolean running = true;

	/**
	 * 构造函数，此构造函数用于共享线程池的处理。
	 *
	 * @param command
	 * @param capacity
	 * @param executor
	 */
	public InBgExecutorQueue(Command<T> command,
			ExecutorService executor) {
		this.command = command;
		this.executor = executor;
	}

	/**
	 * 启动线程。
	 */
	public void start() {
	}


	/** returns the size of the queue. */
	public long getQueueSize() {
		return 0L;
	}

	/** Queue a trap to be processed. */
	public boolean offer(T e) {
		if (e == null) {
			// 不可能发生
			log.warn("offer null.");
			return false;
		}
		// log.info("take data to do, " + getName() + ", " + task);
		executor.execute(new CommandThread(e));
		return true;
	}

	/** stop the processor thread. */
	public void signalStop() {
		running = false;
		if (executor != null) {
			try {
				executor.shutdownNow();
			} catch (Exception e) {
				log.warn(null, e);
			}
		}
	}

	public int getCapacity() {
		return 0;
	}


	class CommandThread implements RejectedRunnableTask {
		T task;
		CommandThread(T e) {
			this.task = e;
		}

		@Override
		public void run() {
			if (log.isTraceEnabled()) {
				log.trace("invoke " + getClass().getName()
						+ ".doCommand in background.");
			}
			try {
				// 实际执行命令。
				command.newCommand().doCommand(task);
			} catch (RejectedExecutionException e) {
				log.warn(
						String.format("queue full, ignored %s",
								String.valueOf(task)), e);
			} catch (Exception e) {
				log.warn("running = " + running, e);
			}
			if (log.isTraceEnabled()) {
				log.trace("invoke command.doCommand in background,  succeeded.");
			}
		}

		@Override
		public void rejected(ThreadPoolExecutor executor) {
			log.warn(String.format("queue full, ignored %s", String.valueOf(task)));
		}

	}


	/**
	 * 内部线程，目的用于封装多线程调用。
	 *
	 */
//	class MultiDequeueThread extends Thread {
//		MultiDequeueThread() {
//			setDaemon(true);
//		}
//
//		/**
//		 * Thread run() method
//		 */
//		public void run() {
//			// log.info("begin invoke doCommand in executor, " + getName());
//			while (running) {
//				try {
//					final T task = theQueue.take();
//					if (task == null) {
//						// 不可能发生
//						log.warn("take null value from queue.");
//						continue;
//					}
//					// log.info("take data to do, " + getName() + ", " + task);
//					executor.execute(new Runnable() {
//						@Override
//						public void run() {
//							if(log.isTraceEnabled()) {
//								log.trace("invoke " + getClass().getName()
//										+ ".doCommand in background, " + getName());
//							}
//							try {
//								// 实际执行命令。
//								command.newCommand().doCommand(task);
//							} catch(RejectedExecutionException e) {
//								log.warn("queue full, " + String.valueOf(task) + " is rejected.", e);
//							} catch (Exception e) {
//								log.warn("running = " + running, e);
//							}
//							if(log.isTraceEnabled()) {
//								log.trace("invoke command.doCommand in background,  succeeded.");
//							}
//						}
//					});
//
//				} catch (InterruptedException e) {
//					log.trace("Interrupted, " + e);
//				} catch (Exception e) {
//					log.warn("running = " + running, e);
//				}
//			} // while...
//			log.info("Quit thread, " + getName());
//		}
//
//	}
} // end
