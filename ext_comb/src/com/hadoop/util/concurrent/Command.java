package com.hadoop.util.concurrent;

/**
 * 异步线程队列中实际在后台被执行的动作。
 * @param <E>
 * 
 * 
 */
public interface Command<E> {

	/**
	 * 实际执行。
	 * @param param
	 */
	void doCommand(E req);
	
	/**
	 * 若单例返回this，否则返回新实例。
	 * @return
	 */
	Command<E> newCommand();

}
