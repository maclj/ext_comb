package com.hadoop.util.concurrent;

/**
 * 自定义的Runtime中断异常，用于Command之类接口抛出。
 *
 * 
 */
public class InterruptedRuntimeException extends RuntimeException {

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = -8562268484867944197L;
	
	public InterruptedRuntimeException() {
		super();
	}
	
	public InterruptedRuntimeException(String message) {
		super(message);
	}
	
	public InterruptedRuntimeException(String message, Throwable cause) {
		super(message, cause);
	}

    public InterruptedRuntimeException(Throwable cause) {
        super(cause);
    }
}
