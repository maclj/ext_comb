package com.hadoop.entry.concurrent;

import java.util.Collection;

/**
 * 并发任务Builder，屏蔽并发任务的后端实现细节。
 * 当前仅支持一次性的批量阻塞任务（目前只用到此场景）
 * 
 * @version 0.1 支持一次性批量阻塞任务。
 *
 * @param <T>
 */
public class ConTasksBuilder<T> {
	
	/**
	 * 实际使用的任务池实现。
	 */
	private ConTasksAdapter<T> tasks = new ConTasksOnce<T>();

	/**
	 * 设置该任务池是一次性任务池还是服务性质任务池（生存周期等同于进程）
	 * @param b true,一次性任务池，当前版本都为一次性任务池。
	 * @return
	 */
	public ConTasksBuilder<T> once(boolean b) {
		tasks.setOnce(b);
		return this;
	}
	
	/**
	 * 设置该任务池是否为阻塞性质任务池。当前版本都为阻塞任务池。
	 * @param b true 阻塞任务池，即只有线程池中的任务都运行结束，才能继续调用线程。
	 * @return
	 */
	public ConTasksBuilder<T> blocking(boolean b) {
		tasks.setBlocking(b);
		return this;
	}
	
	/**
	 * 设置超时时间，为0时则一直等待所有任务结束。
	 * @param timeout 超时时长，单位为秒。
	 * @return
	 */
	public ConTasksBuilder<T> timeout(int timeout) {
		if(timeout < 0) {
			throw new IllegalArgumentException("invalid timeout, " + timeout);
		}
		tasks.setTimeout(timeout);
		return this;
	}
	
	/**
	 * 设定任务池线程个数，不能小于0.缺省值为4.
	 * @param threadNum
	 * @return
	 */
	public ConTasksBuilder<T> threads(int threadNum) {
		if(threadNum < 0) {
			throw new IllegalArgumentException("invalid threadNum, " + threadNum);
		}
		tasks.setThreadNum(threadNum);
		return this;
	}
	
	/**
	 * 设置每个线程启动的延时间隔
	 * @param delay
	 * @return
	 */
	public ConTasksBuilder<T> delay(int delay) {
	    if(delay < 0) {
            throw new IllegalArgumentException("invalid delay, " + delay);
        }
        tasks.setDelay(delay);
        return this;
	}
	
	/**
	 * 设置实际执行的动作实现，不允许为NULL。运行前检查。
	 * @param cmd
	 * @return
	 */
	public ConTasksBuilder<T> addCommand(ConTaskCmd<T> cmd) {
		tasks.setCmd(cmd);
		return this;
	}
	
	/**
	 * 设置被执行的数据，不允许为空
	 * @param data
	 * @return
	 */
	public ConTasksBuilder<T> addData(T data) {
		if (data == null) {
			throw new NullPointerException("empty data.");
		}
		this.tasks.addData(data);
		return this;
	}
	
	/**
	 * 设置被执行的数据，不允许为空
	 * @param datas
	 * @return
	 */
	public ConTasksBuilder<T> addDatas(Collection<T> datas) {
		if (datas == null || datas.isEmpty()) {
			throw new NullPointerException("empty datas.");
		}
		this.tasks.addDatas(datas);
		return this;
	}
	
	/**
	 * 返回构建的任务池对象。
	 * 请在完成所有构建动作后再调用build方法，后续根据条件不同，部分操作将仅能在build方法中实现。
	 * @return
	 */
	public ConTasks build() {
		return this.tasks;
	}
}
