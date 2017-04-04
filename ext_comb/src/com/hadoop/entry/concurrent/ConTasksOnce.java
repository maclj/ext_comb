package com.hadoop.entry.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.hadoop.util.concurrent.Command;
import com.hadoop.util.concurrent.InBgQueue;
import com.hadoop.util.concurrent.InBgThreadQueue;

/**
 * 一次性并行任务，结束后即关闭。
 * 
 * 
 *
 */
public class ConTasksOnce<T> extends ConTasksAdapter<T> {
	
	/** 等待的超时时长 */
	private int timeout;
	
	/** 线程池线程个数  */
	private int threadNum;
	
	/** 消费数据的动作实现  */
	private ConTaskCmd<T> cmd;
	
	/** 被消费的数据 */
	private List<T> datas = new ArrayList<T>();
	
	/** 计数器  */
	private CountDownLatch cdl;
	
	/** 后台线程池 */
	private InBgQueue<ConTask<T>> queue;
	
	/** 不同线程间的启动间隔 */
	private int delay = 0;
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
	public void execute() {
		try {
			// 有效参数检查
			check();
			// 初始化计数器。
			cdl = new CountDownLatch(this.datas.size());
			
			// 构建后台线程池
			queue = new InBgThreadQueue(new ConTaskConsumer(), "ConTaskConsumer", this.datas.size(),
					this.threadNum);
			// 添加任务数据
			ConTask<T> task = null;
			int index = 0;
			for(T data : datas) {
				task = new ConTask<T>(data, cmd, (index ++) * this.delay);
				queue.offer(task);
			}
			// 启动线程池
			queue.start();
			
			// 非阻塞模式，则直接返回
			if(!isBlocking()) {
				return;
			}
			
			// 阻塞的情况下，判断超时时长。
			if (this.timeout == 0) {
				cdl.await();
			} else {
				boolean rs = cdl.await(timeout, TimeUnit.SECONDS);
				if(!rs) {
				    // NOTHING
				}
			}
		} catch (InterruptedException e) {
			//ignored 
		}
		
	}
	

	@Override
	public void close() {
		
		// 一次性 才需要清理资源，服务的话，可以后续继续使用。
		if(this.queue != null) {
			this.queue.signalStop();
		}
		this.cmd = null;
		this.datas = null;
		this.queue = null;
		log.info("ConTasks is closed ...");
	}
	
	/**
	 * 检查运行必备条件
	 */
	private void check() {
		if(!isBlocking()) {
			throw new IllegalArgumentException("not supported, isBlocking = " + isBlocking());
		}
		if(!isOnce()) {
			throw new IllegalArgumentException("not supported, isOnce = " + isOnce());
		}
		if(getTimeout() < 0) {
			throw new IllegalArgumentException("not supported, timeout = " + getTimeout());
		}
		if(getThreadNum() < 1) {
			setThreadNum(4); // 默认设置为4个线程（也可以设置为CPU核数）
		}
		if (getCmd() == null) {
			throw new NullPointerException("empty command.");
		}
		if (this.datas.isEmpty()) {
			throw new NullPointerException("empty datas.");
		}
	}
	

	@Override
	protected boolean isBlocking() {
		return true;
	}

	@Override
	protected void setBlocking(boolean isBlocking) {
		// do nothing
		// 该任务就是阻塞任务。
	}

	@Override
	protected boolean isOnce() {
		return true;
	}

	@Override
	public void setOnce(boolean isOnce) {
		// do nothing
		// 该任务就是一次性任务。
	}
	
	@Override
	protected int getTimeout() {
		return timeout;
	}

	@Override
	protected void setTimeout(int timeout) {
		this.timeout = timeout;
	}
	
	@Override
	protected int getThreadNum() {
		return threadNum;
	}
	
	@Override
	protected void setThreadNum(int threadNum) {
		this.threadNum = threadNum;
	}
	
	@Override
	protected ConTaskCmd<T> getCmd() {
		return cmd;
	}
	@Override
	protected void setCmd(ConTaskCmd<T> cmd) {
		this.cmd = cmd;
	}

	@Override
	protected void addData(T data) {
		this.datas.add(data);
	}
	
	@Override
	protected void addDatas(Collection<T> datas) {
		this.datas.addAll(datas);
	}
	
	public int getDelay() {
        return delay;
    }

	@Override
    public void setDelay(int delay) {
        this.delay = delay;
    }


    @Override
    public String toString() {
        return "ConTasksOnce [timeout=" + timeout + ", threadNum=" + threadNum + ", cmd=" + cmd + ", datas=" + datas
                + ", cdl=" + cdl + ", queue=" + queue + ", delay=" + delay + "]";
    }
	
	/**
	 * 后台线程池消费数据的Command实现。
	 * 
	 *
	 */
	private class ConTaskConsumer implements Command<ConTask<T>> {

        @Override
		public void doCommand(ConTask<T> task) {
			try {
				if(task == null) {
					log.warn("ignored empty task");
					return;
				}
				T data = task.getData();
				ConTaskCmd<T> cmd = task.getCmd();
				if(cmd == null) {
					log.warn("ignored empty cmd");
					return;
				}
				try {
		            Thread.sleep(task.getDelay() * 1000);
		        } catch (InterruptedException e) {
		            // ignored
		        }
				cmd.execute(data, false);
			} finally {
				if(cdl != null) {
					cdl.countDown();
				}
			}
		}

		@Override
		public Command<ConTask<T>> newCommand() {
			return this;
		}
		
	}

}
