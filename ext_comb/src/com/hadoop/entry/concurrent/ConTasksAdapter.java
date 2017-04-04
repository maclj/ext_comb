package com.hadoop.entry.concurrent;

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 任务池适配器，提供平台开发自身需要的方法定义。
 * 
 *
 */
public abstract class ConTasksAdapter<T> implements ConTasks {
	
	/** 日志 */
	protected Log log = LogFactory.getLog(getClass());

	/**
	 * 是否为阻塞实现，缺省为true。
	 * @return
	 */
	protected boolean isBlocking() {
		return true;
	}

	protected void setBlocking(boolean value) {
	}

	/**
	 * 是否为一次性调用，缺省为true。
	 * @return
	 */
	protected boolean isOnce() {
		return true;
	}

	protected void setOnce(boolean value) {
	}

	/**
	 * 超时时长，缺省为0，代表无超时时长。
	 * @return
	 */
	protected int getTimeout() {
		return 0;
	}

	/**
	 * 设置超时时长。
	 * @param timeout 单位秒
	 */
	protected void setTimeout(int timeout) {
	}

	/**
	 * 获取并发线程数量，缺省值为1.
	 * @return
	 */
	protected int getThreadNum() {
		return 1;
	}

	protected void setThreadNum(int threads) {
	}

	protected abstract ConTaskCmd<T> getCmd();

	protected abstract void setCmd(ConTaskCmd<T> cmd);

	protected abstract void addData(T data);

	protected abstract void addDatas(Collection<T> datas);
	
	/** 设置每个任务启动的间隔（逐个递增） ，单位秒*/
	protected abstract void setDelay(int delay);

	@Override
	public void execute() {
	}

	@Override
	public void close() {
	}

}
