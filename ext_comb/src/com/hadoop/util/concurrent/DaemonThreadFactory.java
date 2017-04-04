package com.hadoop.util.concurrent;

import java.util.concurrent.ThreadFactory;

/**
 * 将线程设置成Daemon的缺省工厂。
 * 
 * 
 */
public class DaemonThreadFactory implements ThreadFactory {
	
	/** 线程名称 */
	private String threadName;
	
	public DaemonThreadFactory() {
	}
	
	public DaemonThreadFactory(String threadName) {
		this.threadName = threadName;
	}
	
	@Override
	public Thread newThread(Runnable r) {
		Thread t = new Thread(r);
		t.setDaemon(true);
		if(threadName != null) {
			t.setName(this.threadName + "-" + t.getId());
		}
		return t;
	}

}
