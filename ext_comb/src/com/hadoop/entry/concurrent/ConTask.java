package com.hadoop.entry.concurrent;

/**
 * 并发任务的封装，包含任务数据和处理数据的实现。<br>
 * 
 * 
 *
 */
class ConTask<T> {

    /** 延迟启动的时间，用于 hadoop的并行job启动 */
    private int delay; 
    
	/** 任务数据 */
	private T data;
	
	/** 任务动作  */
	private ConTaskCmd<T> cmd;

	public ConTask(T data, ConTaskCmd<T> cmd, int delay) {
		this.data = data;
		this.cmd = cmd;
		this.delay = delay;
	}

	@Override
	public String toString() {
		return "ConTask [data=" + data + ", cmd=" + cmd + "]";
	}

	public T getData() {
		return data;
	}

	public ConTaskCmd<T> getCmd() {
		return cmd;
	}
	
	public int getDelay() {
        return delay;
    }

    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((cmd == null) ? 0 : cmd.hashCode());
		result = prime * result + ((data == null) ? 0 : data.hashCode());
		return result;
	}

	@SuppressWarnings("unchecked")
    @Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ConTask<T> other = (ConTask<T>) obj;
		if (cmd == null) {
			if (other.cmd != null)
				return false;
		} else if (!cmd.equals(other.cmd))
			return false;
		if (data == null) {
			if (other.data != null)
				return false;
		} else if (!data.equals(other.data))
			return false;
		return true;
	}
	
}
