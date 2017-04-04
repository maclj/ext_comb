package com.hadoop.util.concurrent;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 无法加入队列的缺省处理。
 *
 * 
 *
 */
public class RejectedExecutionHandlerImpl implements RejectedExecutionHandler {

	// 日志。
	protected Log log = LogFactory.getLog(RejectedExecutionHandlerImpl.class);

	@Override
	public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {

		if (!(r instanceof RejectedRunnableTask)) {
			log.warn(String.format("queue full, %s rejected, %s",
					String.valueOf(r), String.valueOf(executor)));
			return;
		}
		try {
			// 如果实现了该接口，应该自己记queue full日志。
			((RejectedRunnableTask) r).rejected(executor);
		} catch (Exception e) {
			log.warn(null, e);
		}
	}

}
